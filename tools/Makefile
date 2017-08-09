CXX = g++
CXXFLAGS = -O0 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX

SRC_DIR = ./
PB_DIR = ../src/common/

THIRD_PATH = ../third
LEVELDB_PATH = $(THIRD_PATH)/floyd/third/leveldb

ifndef NEMODB_PATH
NEMODB_PATH = $(realpath $(THIRD_PATH)/nemo-rocksdb)
endif

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(realpath $(THIRD_PATH)/rocksdb)
endif

ifndef SLASH_PATH
SLASH_PATH = $(realpath $(THIRD_PATH)/slash)
endif

ifndef PINK_PATH
PINK_PATH = $(realpath $(THIRD_PATH)/pink)
endif

INCLUDE_PATH = -I$(ROCKSDB_PATH)/ \
							 -I$(ROCKSDB_PATH)/include \
							 -I$(SLASH_PATH)/ \
							 -I$(PINK_PATH)/ \
							 -I..

LIB_PATH = -L$(ROCKSDB_PATH)/ \
					 -L$(SLASH_PATH)/slash/lib \
					 -L$(PINK_PATH)/pink/lib

LIBS = -lpthread \
			 -lprotobuf \
			 -lrocksdb \
			 -lpink \
			 -lslash \
			 -lz \
			 -lbz2 \
			 -lsnappy \
			 -lrt

.PHONY: all clean

BASE_OBJS += $(wildcard $(PB_DIR)/zp_meta.pb.cc)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))

OBJECT = dump_meta empty_trash check_binlog_hole
all: $(OBJECT)
	@echo "Success, go, go, go..."

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH)

dump_meta: $(OBJS) dump_meta.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

empty_trash: $(OBJS) empty_trash.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

check_binlog_hole: $(OBJS) check_binlog_hole.cc
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) $(LIBS)

clean: 
	rm -rf ./*.o
	rm $(OBJECT)
