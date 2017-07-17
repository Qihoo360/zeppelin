CXX = g++
CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX

SRC_DIR = ./
PB_DIR = ../src/common/

LEVELDB_PATH = $(THIRD_PATH)/floyd/third/leveldb

THIRD_PATH = ../third

ifndef NEMODB_PATH
NEMODB_PATH = $(realpath $(THIRD_PATH)/nemo-rocksdb)
endif

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(realpath $(THIRD_PATH)/rocksdb)
endif

INCLUDE_PATH = -I$(ROCKSDB_PATH)/ \
							 -I$(ROCKSDB_PATH)/include \
							 -I..


LIB_PATH = -L$(ROCKSDB_PATH)/

LIBS = -lpthread \
			 -lprotobuf \
			 -lrocksdb \
			 -lz \
			 -lbz2 \
			 -lsnappy \
			 -lrt


.PHONY: all clean

BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
BASE_OBJS += $(wildcard $(PB_DIR)/zp_meta.pb.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

all: dump_meta
	@echo "Success, go, go, go..."

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

dump_meta: $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

clean: 
	rm -rf ./*.o
	rm dump_meta
