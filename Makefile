ifneq ($(DEB_HOST_MULTIARCH),)
	CROSS_COMPILE ?= $(DEB_HOST_MULTIARCH)-
endif

ifeq ($(origin CC),default)
	CC := $(CROSS_COMPILE)gcc
endif
ifeq ($(origin CXX),default)
	CXX := $(CROSS_COMPILE)g++
endif

ifeq ($(DEBUG),)
	BUILD_DIR ?= build/release
else
	BUILD_DIR ?= build/debug
endif

PREFIX = /usr

DB_BIN = wb-mqtt-db
SRC_DIR = src

SQLITECPP_SRC = thirdparty/SQLiteCpp/src
SQLITECPP_INCLUDE = thirdparty/SQLiteCpp/include

COMMON_SRCS := $(shell find $(SRC_DIR) $(SQLITECPP_SRC) \( -name *.cpp -or -name *.c \) -and -not -name main.cpp)
COMMON_OBJS := $(COMMON_SRCS:%=$(BUILD_DIR)/%.o)

CXXFLAGS = -Wall -std=c++14 -I$(SRC_DIR) -I$(SQLITECPP_INCLUDE) -Wno-psabi
LDFLAGS = -lwbmqtt1 -lsqlite3 -lpthread

ifeq ($(DEBUG), 1)
	CXXFLAGS+=-O0 -g -fprofile-arcs -ftest-coverage
	LDFLAGS += -lgcov
else
	CXXFLAGS+=-Os -DNDEBUG
endif

TEST_DIR = test
TEST_SRCS := $(shell find $(TEST_DIR) \( -name *.cpp -or -name *.c \))
TEST_OBJS := $(TEST_SRCS:%=$(BUILD_DIR)/%.o)
TEST_BIN=wb-mqtt-db-test
TEST_LIBS=-lgtest -lwbmqtt_test_utils -lpthread

export TEST_DIR_ABS = $(shell pwd)/$(TEST_DIR)

.PHONY: all clean test

all : $(DB_BIN)

$(DB_BIN): $(COMMON_OBJS) $(BUILD_DIR)/src/main.cpp.o
	$(CXX) $^ $(LDFLAGS) -o $(BUILD_DIR)/$@

$(BUILD_DIR)/%.cpp.o: %.cpp
	mkdir -p $(dir $@)
	$(CXX) -c $< -o $@ $(CXXFLAGS)

$(BUILD_DIR)/test/%.o: test/%.cpp
	$(CXX) -c $(CXXFLAGS) -o $@ $^

$(TEST_DIR)/$(TEST_BIN): $(COMMON_OBJS) $(TEST_OBJS)
	$(CXX) $^ $(LDFLAGS) $(TEST_LIBS) -o $@ -fno-lto

test: $(TEST_DIR)/$(TEST_BIN)
	rm -f $(TEST_DIR)/*.dat.out
	if [ "$(shell arch)" != "armv7l" ] && [ "$(CROSS_COMPILE)" = "" ] || [ "$(CROSS_COMPILE)" = "x86_64-linux-gnu-" ]; then \
		valgrind --error-exitcode=180 -q $(TEST_DIR)/$(TEST_BIN) $(TEST_ARGS) || \
		if [ $$? = 180 ]; then \
			echo "*** VALGRIND DETECTED ERRORS ***" 1>& 2; \
			exit 1; \
		else $(TEST_DIR)/abt.sh show; exit 1; fi; \
	else \
		$(TEST_DIR)/$(TEST_BIN) $(TEST_ARGS) || { $(TEST_DIR)/abt.sh show; exit 1; } \
	fi

lcov: test
ifeq ($(DEBUG), 1)
	geninfo --no-external -b . -o $(BUILD_DIR)/coverage.info $(BUILD_DIR)/src
	genhtml $(BUILD_DIR)/coverage.info -o $(BUILD_DIR)/cov_html
endif

clean:
	-rm -rf build/release
	-rm -rf build/debug
	-rm -f $(TEST_DIR)/*.dat.out
	-rm -f $(TEST_DIR)/*.o $(TEST_DIR)/$(TEST_BIN)

install:
	install -d $(DESTDIR)/var/lib/wirenboard/db

	install -Dm0755 $(BUILD_DIR)/$(DB_BIN) -t $(DESTDIR)$(PREFIX)/bin
	install -Dm0644 config.json $(DESTDIR)/etc/wb-mqtt-db.conf

	install -Dm0644 wb-mqtt-db.wbconfigs $(DESTDIR)/etc/wb-configs.d/16wb-mqtt-db
	install -Dm0644 wb-mqtt-db.schema.json -t $(DESTDIR)$(PREFIX)/share/wb-mqtt-confed/schemas

