DEBUG?=0

CXXFLAGS=-Wall -std=c++14 -I. -I./thirdparty/SQLiteCpp/include
LDFLAGS= -ljsoncpp -lwbmqtt1 -lsqlite3

ifeq ($(DEBUG), 1)
	CXXFLAGS+=-ggdb -O0 -pg
	LDFLAGS+=-pg
else
	CXXFLAGS+=-Os -DNDEBUG
endif

CPPFLAGS=$(CFLAGS)

DB_BIN=wb-mqtt-db
SQLITECPP_DIR=thirdparty/SQLiteCpp/src
SQLITECPP_OBJ := $(patsubst %.cpp,%.o,$(wildcard $(SQLITECPP_DIR)/*.cpp))

OBJ=main.o config.o log.o sqlite_storage.o dblogger.o
DB_CONFCONVERT=wb-mqtt-db-confconvert

.PHONY: all clean

all : $(DB_BIN)


$(DB_BIN): $(OBJ) $(SQLITECPP_OBJ)
	${CXX} $^ ${LDFLAGS} -o $@

%.o: %.cpp
	${CXX} -c $< -o $@ ${CXXFLAGS};

clean :
	-rm -f *.o $(DB_BIN)
	-rm -f $(SQLITECPP_DIR)/*.o

install: all
	install -d $(DESTDIR)
	install -d $(DESTDIR)/etc
	install -d $(DESTDIR)/usr/bin
	install -d $(DESTDIR)/usr/lib
	install -d $(DESTDIR)/var/lib/wirenboard
	install -d $(DESTDIR)/var/lib/wirenboard/db
	install -d $(DESTDIR)/usr/share/wb-mqtt-confed/schemas

	install -m 0755  $(DB_BIN) $(DESTDIR)/usr/bin/$(DB_BIN)
	install -m 0755  $(DB_CONFCONVERT) $(DESTDIR)/usr/bin/$(DB_CONFCONVERT)
	install -m 0755  config.json $(DESTDIR)/etc/wb-mqtt-db.conf

	install -m 0644  wb-mqtt-db.wbconfigs $(DESTDIR)/etc/wb-configs.d/16wb-mqtt-db
	install -m 0644  wb-mqtt-db.schema.json $(DESTDIR)/usr/share/wb-mqtt-confed/schemas/wb-mqtt-db.schema.json

