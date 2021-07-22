DEBUG?=0

ifneq ($(DEB_HOST_MULTIARCH),)
	CROSS_COMPILE ?= $(DEB_HOST_MULTIARCH)-
endif

ifeq ($(origin CC),default)
	CC := $(CROSS_COMPILE)gcc
endif
ifeq ($(origin CXX),default)
	CXX := $(CROSS_COMPILE)g++
endif

CXXFLAGS=-Wall -std=c++14 -I. -I./thirdparty/SQLiteCpp/include -Wno-psabi

# We build armhf targets with an old version of sqlite
CC_TARGET := $(shell $(CC) -dumpmachine)
ifeq ($(CC_TARGET),arm-linux-gnueabihf)
	CXXFLAGS+=-DSQLITE_USE_LEGACY_STRUCT
endif

LDFLAGS=-lwbmqtt1 -lsqlite3

ifeq ($(DEBUG), 1)
	CXXFLAGS+=-O0 -g
else
	CXXFLAGS+=-Os -DNDEBUG
endif

CPPFLAGS=$(CFLAGS)

DB_BIN=wb-mqtt-db
SQLITECPP_DIR=thirdparty/SQLiteCpp/src
SQLITECPP_OBJ := $(patsubst %.cpp,%.o,$(wildcard $(SQLITECPP_DIR)/*.cpp))

OBJ=config.o log.o sqlite_storage.o dblogger.o db_migrations.o
DB_CONFCONVERT=wb-mqtt-db-confconvert

TEST_SOURCES= 								\
			$(TEST_DIR)/test_main.cpp		\
			$(TEST_DIR)/config.test.cpp		\
			$(TEST_DIR)/rpc.test.cpp		\
			$(TEST_DIR)/dblogger.test.cpp	\
			$(TEST_DIR)/sqlite_storage.test.cpp	\

TEST_DIR=test
export TEST_DIR_ABS = $(shell pwd)/$(TEST_DIR)

TEST_OBJECTS=$(TEST_SOURCES:.cpp=.o)
TEST_BIN=wb-mqtt-db-test
TEST_LIBS=-lgtest -lwbmqtt_test_utils -lpthread

.PHONY: all clean

all : $(DB_BIN)

$(DB_BIN): main.o $(OBJ) $(SQLITECPP_OBJ)
	${CXX} $^ ${LDFLAGS} -o $@

%.o: %.cpp
	${CXX} -c $< -o $@ ${CXXFLAGS};

$(TEST_DIR)/$(TEST_BIN): $(OBJ) $(SQLITECPP_OBJ) $(TEST_OBJECTS)
	${CXX} $^ $(LDFLAGS) $(TEST_LIBS) -o $@ -fno-lto

test: $(TEST_DIR)/$(TEST_BIN)
	rm -f $(TEST_DIR)/*.dat.out
ifneq ($(DEBUG), 1)
	if [ "$(shell arch)" != "armv7l" ] && [ "$(CROSS_COMPILE)" = "" ] || [ "$(CROSS_COMPILE)" = "x86_64-linux-gnu-" ]; then \
		valgrind --error-exitcode=180 -q $(TEST_DIR)/$(TEST_BIN) $(TEST_ARGS) || \
		if [ $$? = 180 ]; then \
			echo "*** VALGRIND DETECTED ERRORS ***" 1>& 2; \
			exit 1; \
		else $(TEST_DIR)/abt.sh show; exit 1; fi; \
    else \
        $(TEST_DIR)/$(TEST_BIN) $(TEST_ARGS) || { $(TEST_DIR)/abt.sh show; exit 1; } \
	fi
endif

clean:
	-rm -f $(TEST_DIR)/*.dat.out
	-rm -f *.o $(DB_BIN)
	-rm -f $(SQLITECPP_DIR)/*.o
	-rm -f $(TEST_DIR)/*.o $(TEST_DIR)/$(TEST_BIN)

install: all
	install -d $(DESTDIR)/var/lib/wirenboard/db

	install -D -m 0755  $(DB_BIN) $(DESTDIR)/usr/bin/$(DB_BIN)
	install -D -m 0755  $(DB_CONFCONVERT) $(DESTDIR)/usr/bin/$(DB_CONFCONVERT)
	install -D -m 0644  config.json $(DESTDIR)/etc/wb-mqtt-db.conf

	install -D -m 0644  wb-mqtt-db.wbconfigs $(DESTDIR)/etc/wb-configs.d/16wb-mqtt-db
	install -D -m 0644  wb-mqtt-db.schema.json $(DESTDIR)/usr/share/wb-mqtt-confed/schemas/wb-mqtt-db.schema.json

