set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Wiren Board database logger")
set(CPACK_PACKAGE_DESCRIPTION "wb-mqtt-db is a service which stores values from MQTT controls in data base.
 It offers MQTT RPC for retrieving stored data.")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/wirenboard/wb-mqtt-db")
set(CPACK_PACKAGE_VENDOR "Wiren Board")
set(CPACK_PACKAGE_CONTACT "Evgeny Boger <boger@contactless.ru>")

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_SECTION "misc")
set(CPACK_DEBIAN_PACKAGE_BREAKS "wb-mqtt-confed (<< 1.0.2)")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS YES)
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "")

include(CPack)
