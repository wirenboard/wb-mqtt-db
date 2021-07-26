#include "dblogger.h"

#include <getopt.h>

#include "config.h"
#include "log.h"
#include "sqlite_storage.h"

#include <wblib/signal_handling.h>
#include <wblib/wbmqtt.h>
#include <wblib/rpc.h>

using namespace std;
using namespace std::chrono;

namespace
{
    //! Maximum timeout before forced application termination. Topic cleanup can take a lot of time
    const auto DRIVER_STOP_TIMEOUT_S = chrono::seconds(5);

    //! Maximun time to start application. Exceded timeout will case application termination.
    const auto DRIVER_INIT_TIMEOUT_S = chrono::seconds(30);

    const auto APP_NAME = "wb-mqtt-db";

    void PrintUsage()
    {
        cout << "Usage:" << endl
             << " " << APP_NAME <<" [options]" << endl
             << "Options:" << endl
             << "  -d       level     enable debuging output:" << endl
             << "                       1 - db only;" << endl
             << "                       2 - mqtt only;" << endl
             << "                       3 - both;" << endl
             << "                       negative values - silent mode (-1, -2, -3))" << endl
             << "  -c       config    config file" << endl
             << "  -p       port      MQTT broker port (default: 1883)" << endl
             << "  -h, -H   IP        MQTT broker IP (default: localhost)" << endl
             << "  -u       user      MQTT user (optional)" << endl
             << "  -P       password  MQTT user password (optional)" << endl
             << "  -T       prefix    MQTT topic prefix (optional)" << endl;
    }

    void ParseCommadLine(int                           argc,
                         char*                         argv[],
                         WBMQTT::TMosquittoMqttConfig& mqttConfig,
                         string&                       config)
    {
        int debugLevel = 0;
        int c;
        while ((c = getopt(argc, argv, "d:c:h:H:p:u:P:T:")) != -1) {
            switch (c) {
            case 'd':
                debugLevel = stoi(optarg);
                break;
            case 'c':
                config = optarg;
                break;
            case 'p':
                mqttConfig.Port = stoi(optarg);
                break;
            case 'h':
            case 'H': // backward compatibility
                mqttConfig.Host = optarg;
                break;
            case 'T':
                mqttConfig.Prefix = optarg;
                break;
            case 'u':
                mqttConfig.User = optarg;
                break;
            case 'P':
                mqttConfig.Password = optarg;
                break;

            case '?':
            default:
                PrintUsage();
                exit(2);
            }
        }

        switch (debugLevel) {
        case 0:
            break;
        case -1:
            Info.SetEnabled(false);
            break;

        case -2:
            WBMQTT::Info.SetEnabled(false);
            break;

        case -3:
            WBMQTT::Info.SetEnabled(false);
            Info.SetEnabled(false);
            break;

        case 1:
            Debug.SetEnabled(true);
            break;

        case 2:
            WBMQTT::Debug.SetEnabled(true);
            break;

        case 3:
            WBMQTT::Debug.SetEnabled(true);
            Debug.SetEnabled(true);
            break;

        default:
            cout << "Invalid -d parameter value " << debugLevel << endl;
            PrintUsage();
            exit(2);
        }

        if (optind < argc) {
            for (int index = optind; index < argc; ++index) {
                cout << "Skipping unknown argument " << argv[index] << endl;
            }
        }
    }

    void PrintStartupInfo(const WBMQTT::TMosquittoMqttConfig& mqttConfig, const string& configFile)
    {
        cout << "MQTT broker " << mqttConfig.Host << ':' << mqttConfig.Port << endl;
        cout << "Config file " << configFile << endl;
    }

} // namespace

int main(int argc, char* argv[])
{
    WBMQTT::TMosquittoMqttConfig mqttConfig;
    mqttConfig.Id = APP_NAME;

    string configFileName("/etc/wb-mqtt-db.conf");

    ParseCommadLine(argc, argv, mqttConfig, configFileName);
    PrintStartupInfo(mqttConfig, configFileName);

    WBMQTT::TPromise<void> initialized;
    WBMQTT::SetThreadName("main");
    WBMQTT::SignalHandling::Handle({SIGINT, SIGTERM});
    WBMQTT::SignalHandling::OnSignals({SIGINT, SIGTERM}, [&] { WBMQTT::SignalHandling::Stop(); });

    /* if signal arrived before driver is initialized:
        wait some time to initialize and then exit gracefully
        else if timed out: exit with error
    */
    WBMQTT::SignalHandling::SetWaitFor(DRIVER_INIT_TIMEOUT_S, initialized.GetFuture(), [&] {
        Error.Log() << "Driver takes too long to initialize. Exiting.";
        cerr << "Error: DRIVER_INIT_TIMEOUT_S" << endl;
        exit(1);
    });

    /* if handling of signal takes too much time: exit with error */
    WBMQTT::SignalHandling::SetOnTimeout(DRIVER_STOP_TIMEOUT_S, [&] {
        Error.Log() << "Driver takes too long to stop. Exiting.";
        cerr << "Error: DRIVER_STOP_TIMEOUT_S" << endl;
        exit(2);
    });
    WBMQTT::SignalHandling::Start();

    try {
        auto config = LoadConfig(configFileName, "/usr/share/wb-mqtt-confed/schemas/wb-mqtt-db.schema.json");
        auto mqttClient(WBMQTT::NewMosquittoMqttClient(mqttConfig));
        auto backend = WBMQTT::NewDriverBackend(mqttClient);
        auto driver = WBMQTT::NewDriver(WBMQTT::TDriverArgs{}.SetId(APP_NAME).SetBackend(backend));
        auto rpcServer = WBMQTT::NewMqttRpcServer(mqttClient, "db_logger");
        std::shared_ptr<TMQTTDBLogger> logger(
            new TMQTTDBLogger(driver,
                              config.Cache,
                              std::make_unique<TSqliteStorage>(config.DBFile),
                              rpcServer,
                              std::make_unique<TChannelWriter>(),
                              config.GetValuesRpcRequestTimeout));

        WBMQTT::SignalHandling::OnSignals({SIGINT, SIGTERM}, [=] { logger->Stop(); });
        initialized.Complete();
        Info.Log() << "DB logger started, go to main loop";
        logger->Start();
    } catch (const std::exception& e) {
        Error.Log() << e.what();
        WBMQTT::SignalHandling::Stop();
        return 2;
    }
    WBMQTT::SignalHandling::Wait();
    return 0;
}
