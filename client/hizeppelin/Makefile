RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

CXX = g++
#ifeq ($(__PERF), 1)
CXXFLAGS = -O2 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
#else
#	CXXFLAGS = -O2 -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-unused-variable
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ 
#endif
OBJECT = zp_manager zp_client zp_benchmark zp_create_table 
SRC_DIR = ./
OUTPUT = ./output
THIRD_PATH = ../../third



LIB_PATH = -L../output/lib/ \
		   -L../third/linenoise/ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/  \
					 -L/usr/local/lib/

LIBS = -lzp \
			 -lprotobuf \
			 -lpink \
			 -lslash \
			 -lpthread

INCLUDE_PATH = -I../output/include/ \
		   -I../output/  \
		   -I../third/linenoise/  \
		   -I../third/  \
		   -I$(THIRD_PATH)/pink/output/  \
		   -I$(THIRD_PATH)/pink/output/include/ \
		   -I$(THIRD_PATH)/slash/output/  \
		   -I$(THIRD_PATH)/slash/output/include/

.PHONY: all clean version


BASE_BOJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_BOJS += $(wildcard $(SRC_DIR)/*.c)
BASE_BOJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_BOJS))

all: $(OBJECT)
	rm -rf $(OUTPUT)
	mkdir -p $(OUTPUT)
	mkdir -p $(OUTPUT)/bin
	mkdir -p $(OUTPUT)/log
	cp $(OBJECT) $(OUTPUT)/bin/
	rm -rf $(OBJECT)
	@echo "Success, go, go, go..."


zp_cli: zp_cli.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_manager: zp_manager.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ ../third/linenoise/linenoise.o $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_client: zp_client.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_create_table: zp_create_table.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

zp_benchmark: zp_benchmark.cc
	$(CXX) $(CXXFLAGS) -o $@ $^  $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)

qps_test: qps_test.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS)



clean: 
	rm -rf $(OUTPUT)
	rm -rf $(SRC_DIR)/log
	rm -f $(SRC_DIR)/*.o
	rm -rf $(OBJECT)

