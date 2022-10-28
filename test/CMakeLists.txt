find_package(GTest REQUIRED)
include(GoogleTest)

set(TEST_LIST
    config.test.cpp
    dblogger.test.cpp
    rpc.test.cpp
    sqlite_storage.test.cpp
)

add_executable(${PROJECT_NAME}_test ${TEST_LIST})
target_link_libraries(${PROJECT_NAME}_test PRIVATE GTest::GTest GTest::Main mqtt-db sqlite3 sqlitecpp wbmqtt1 wbmqtt_test_utils pthread)
target_include_directories(${PROJECT_NAME}_test PRIVATE ${PROJECT_SOURCE_DIR})

gtest_discover_tests(${PROJECT_NAME}_test
                     WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/test)
