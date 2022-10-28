cmake_minimum_required(VERSION 3.18)

project(wb-mqtt-db VERSION 2.8.2)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_compile_options(-Wall)

set(MQTT_DB_SOURCES
    benchmark.cpp
    config.cpp
    db_migrations.cpp
    dblogger.cpp
    log.cpp
    sqlite_storage.cpp
    storage.cpp
)

set(SQLITECPP_SOURCES
    thirdparty/SQLiteCpp/src/Backup.cpp
    thirdparty/SQLiteCpp/src/Column.cpp
    thirdparty/SQLiteCpp/src/Database.cpp
    thirdparty/SQLiteCpp/src/Exception.cpp
    thirdparty/SQLiteCpp/src/Statement.cpp
    thirdparty/SQLiteCpp/src/Transaction.cpp
)

include_directories(thirdparty/SQLiteCpp/include)

add_library(mqtt-db STATIC ${MQTT_DB_SOURCES})
add_library(sqlitecpp STATIC ${SQLITECPP_SOURCES})

add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries(${PROJECT_NAME} PRIVATE mqtt-db sqlite3 sqlitecpp wbmqtt1 pthread)

include(CTest)
if(BUILD_TESTING)
    add_subdirectory(test)
endif()

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(Installing)
include(Packing)
