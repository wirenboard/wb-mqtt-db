#include "db_migrations.h"

namespace
{
    void ConvertDb0To1(SQLite::Database& db)
    {
        db.exec("ALTER TABLE data RENAME TO tmp");

        // drop existing indexes
        db.exec("DROP INDEX data_topic");
        db.exec("DROP INDEX data_topic_timestamp");
        db.exec("DROP INDEX data_gid");
        db.exec("DROP INDEX data_gid_timestamp");

        // create tables
        db.exec("CREATE TABLE IF NOT EXISTS devices ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device VARCHAR(255) UNIQUE "
                " )  ");

        db.exec("CREATE TABLE IF NOT EXISTS channels ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device VARCHAR(255), "
                "control VARCHAR(255) "
                ")  ");

        db.exec("CREATE TABLE IF NOT EXISTS groups ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "group_id VARCHAR(255) "
                ")  ");


        db.exec("CREATE TABLE IF NOT EXISTS data ("
                "uid INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device INTEGER,"
                "channel INTEGER,"
                "value VARCHAR(255),"
                "timestamp REAL DEFAULT(julianday('now')),"
                "group_id INTEGER"
                ")" );

        db.exec("CREATE TABLE IF NOT EXISTS variables ("
                "name VARCHAR(255) PRIMARY KEY, "
                "value VARCHAR(255) )" );

        // generate internal integer ids from old data table
        db.exec("INSERT OR IGNORE INTO devices (device) SELECT device FROM tmp GROUP BY device");
        db.exec("INSERT OR IGNORE INTO channels (device, control) SELECT device, control FROM tmp "
                    "GROUP BY device, control");
        db.exec(
            "INSERT OR IGNORE INTO groups (group_id) SELECT group_id FROM tmp GROUP BY group_id");

        // populate data table using values from old data table
        db.exec(
            "INSERT INTO data(uid, device, channel,value,timestamp,group_id) "
            "SELECT uid, devices.int_id, channels.int_id, value, julianday(timestamp), groups.int_id "
            "FROM tmp "
            "LEFT JOIN devices ON tmp.device = devices.device "
            "LEFT JOIN channels ON tmp.device = channels.device AND tmp.control = channels.control "
            "LEFT JOIN groups ON tmp.group_id = groups.group_id ");

        db.exec("DROP TABLE tmp");

        db.exec("UPDATE variables SET value=\"1\" WHERE name=\"db_version\"");
    }

    void ConvertDb1To2(SQLite::Database& db)
    {
        // In versions >= 2, there is a difference in 'data' table:
        // add data.max, data.min columns
        db.exec("ALTER TABLE data ADD COLUMN max VARCHAR(255)");
        db.exec("ALTER TABLE data ADD COLUMN min VARCHAR(255)");
        db.exec("ALTER TABLE data ADD COLUMN retained INTEGER");

        db.exec("UPDATE data SET max = value");
        db.exec("UPDATE data SET min = value");
        db.exec("UPDATE data SET retained = 0");

        db.exec("UPDATE variables SET value=\"2\" WHERE name=\"db_version\"");
    }

    void ConvertDb2To3(SQLite::Database& db)
    {
        // save old data table
        db.exec("ALTER TABLE data RENAME TO data_old");

        // create new data table
        db.exec("CREATE TABLE data ("
                    "uid INTEGER PRIMARY KEY AUTOINCREMENT,"
                    "device INTEGER,"
                    "channel INTEGER,"
                    "value VARCHAR(255),"
                    "timestamp INTEGER DEFAULT(0),"
                    "group_id INTEGER,"
                    "max VARCHAR(255),"
                    "min VARCHAR(255),"
                    "retained INTEGER"
                    ")");

        // copy all data casting timestamps
        db.exec("INSERT INTO data "
                    "SELECT uid, device, channel, value, "
                    "CAST((timestamp - 2440587.5) * 86400000 AS INTEGER), group_id, max, min, retained "
                    "FROM data_old");

        // drop old data table
        db.exec("DROP TABLE data_old");

        db.exec("UPDATE variables SET value=\"3\" WHERE name=\"db_version\"");
    }

    void ConvertDb3To4(SQLite::Database& db)
    {
        // save old channels table
        db.exec("ALTER TABLE channels RENAME TO channels_old");

        // save new channels table
        db.exec("CREATE TABLE channels ( "
                "int_id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "device VARCHAR(255), "
                "control VARCHAR(255), "
                "UNIQUE(device,control) "
                ")");

        // save unique channels to new channels table and update keys in data table
        {
            SQLite::Statement query(
                db,
                "SELECT int_id, device, control FROM channels_old ORDER BY device, control");
            int          prevId = -1;
            std::string  prevDevice;
            std::string  prevControl;
            while (query.executeStep()) {
                std::string curDevice(query.getColumn(1).getString());
                std::string curControl(query.getColumn(2).getString());
                if ((curDevice == prevDevice) && (curControl == prevControl)) {
                    SQLite::Statement updateQuery(db, "UPDATE data SET channel=? WHERE channel=?");
                    updateQuery.bind(1, prevId);
                    updateQuery.bind(2, query.getColumn(0).getInt());
                    updateQuery.exec();
                } else {
                    prevDevice  = curDevice;
                    prevControl = prevControl;
                    prevId      = query.getColumn(0).getInt();
                    SQLite::Statement insertQuery(
                        db,
                        "INSERT INTO channels(int_id, device, control) VALUES(?, ?, ?)");
                    insertQuery.bind(1, prevId);
                    insertQuery.bind(2, curDevice);
                    insertQuery.bind(3, curControl);
                    insertQuery.exec();
                }
            }
        }

        // drop old channels table
        db.exec("DROP TABLE channels_old");

        // save old data table
        db.exec("ALTER TABLE data RENAME TO data_old");

        // create new data table
        db.exec("CREATE TABLE data ("
                "uid INTEGER PRIMARY KEY AUTOINCREMENT,"
                "channel INTEGER,"
                "value VARCHAR(255),"
                "timestamp INTEGER DEFAULT(0),"
                "group_id INTEGER,"
                "max VARCHAR(255),"
                "min VARCHAR(255),"
                "retained INTEGER"
                ")");

        // copy all data without device field
        db.exec("INSERT INTO data "
                "SELECT uid, channel, value, timestamp, group_id, max, min, retained "
                "FROM data_old");

        // drop old data table
        db.exec("DROP TABLE data_old");

        // drop old devices table
        db.exec("DROP TABLE devices");

        db.exec("UPDATE variables SET value=\"4\" WHERE name=\"db_version\"");
    }

    void ConvertDb4To5(SQLite::Database& db)
    {
        // In version 5 groups table and group_id field in data table are removed

        // save old data table
        db.exec("ALTER TABLE data RENAME TO data_old");

        // create new data table
        db.exec("CREATE TABLE data ("
                "uid INTEGER PRIMARY KEY AUTOINCREMENT,"
                "channel INTEGER,"
                "value VARCHAR(255),"
                "timestamp INTEGER DEFAULT(0),"
                "max VARCHAR(255),"
                "min VARCHAR(255),"
                "retained INTEGER"
                ")");

        // copy all data without device field
        db.exec("INSERT INTO data "
                "SELECT uid, channel, value, timestamp, max, min, retained "
                "FROM data_old");

        // add precision column to channel
        db.exec("ALTER TABLE channel ADD COLUMN precision REAL");

        // drop old data table
        db.exec("DROP TABLE data_old");

        // drop groups table
        db.exec("DROP TABLE groups");

        db.exec("UPDATE variables SET value=\"5\" WHERE name=\"db_version\"");
    }
}

std::vector<ConvertDbFnType> GetMigrations()
{
    return {ConvertDb0To1,
            ConvertDb1To2,
            ConvertDb2To3,
            ConvertDb3To4,
            ConvertDb4To5};
}
