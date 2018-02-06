CXX= g++
CXXFLAGS= -g -pipe -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -DOS_LINUX
LDFLAGS= -lpthread -lprotobuf -lz -lbz2 -lsnappy -lrt

ZP_SRC_PATH= $(CURDIR)/../src

# ----------------Dependences-------------------
THIRD_PATH = $(CURDIR)/../third

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(realpath $(THIRD_PATH)/rocksdb)
endif
LIBROCKSDB = $(ROCKSDB_PATH)/librocksdb.a

ifndef SLASH_PATH
SLASH_PATH = $(realpath $(THIRD_PATH)/slash)
endif
LIBSLASH = $(SLASH_PATH)/slash/lib/libslash.a

ifndef PINK_PATH
PINK_PATH = $(realpath $(THIRD_PATH)/pink)
endif
LIBPINK = $(PINK_PATH)/pink/lib/libpink.a

PROTODIR = $(THIRD_PATH)/protobuf
LIBPROTOBUF = $(PROTODIR)/_install/lib/libprotobuf.a
PROTOC = $(PROTODIR)/_install/bin/protoc

INCLUDE_PATH = -I$(ROCKSDB_PATH)/include \
							 -I$(SLASH_PATH) \
							 -I$(PINK_PATH) \
							 -I..

COMMON_DEPS= $(LIBPINK) $(LIBSLASH) $(LIBROCKSDB)
# ---------------End Dependences----------------

AM_DEFAULT_VERBOSITY= 0

AM_V_GEN = $(am__v_GEN_$(V))
am__v_GEN_ = $(am__v_GEN_$(AM_DEFAULT_VERBOSITY))
am__v_GEN_0 = @echo "  GEN     " $(notdir $@);
am__v_GEN_1 =
AM_V_at = $(am__v_at_$(V))
am__v_at_ = $(am__v_at_$(AM_DEFAULT_VERBOSITY))
am__v_at_0 = @
am__v_at_1 =

AM_V_CC = $(am__v_CC_$(V))
am__v_CC_ = $(am__v_CC_$(AM_DEFAULT_VERBOSITY))
am__v_CC_0 = @echo "  CC      " $(notdir $@);
am__v_CC_1 =
CCLD = $(CC)
LINK = $(CCLD) $(AM_CFLAGS) $(CFLAGS) $(AM_LDFLAGS) $(LDFLAGS) -o $@
AM_V_CCLD = $(am__v_CCLD_$(V))
am__v_CCLD_ = $(am__v_CCLD_$(AM_DEFAULT_VERBOSITY))
am__v_CCLD_0 = @echo "  CCLD    " $(notdir $@);
am__v_CCLD_1 =
#-----------------------------------------------

.PHONY: all clean proto

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

CXXFLAGS += $(WARNING_FLAGS) $(INCLUDE_PATH)

META_PROTO = $(wildcard $(ZP_SRC_PATH)/meta/*.proto)
META_PROTO_GENS = $(META_PROTO:%.proto=%.pb.cc) $(META_PROTO:%.proto=%.pb.h)
META_PROTO_SRC = $(META_PROTO:%.proto=%.pb.cc)

NODE_PROTO = $(wildcard $(ZP_SRC_PATH)/node/*.proto)
NODE_PROTO_GENS = $(NODE_PROTO:%.proto=%.pb.cc) $(NODE_PROTO:%.proto=%.pb.h)
NODE_PROTO_SRC = $(NODE_PROTO:%.proto=%.pb.cc)

BINARIES = dump_meta empty_trash check_binlog_hole checknfix binlog_reader
all: $(BINARIES)
	$(AM_V_at)echo "Success, go, go, go..."

%.pb.cc %.pb.h: %.proto $(PROTOC)
	$(AM_V_GEN)
	$(AM_V_at)$(PROTOC) -I$(dir $<) --cpp_out=$(dir $<) $<

proto: $(META_PROTO_GENS) $(NODE_PROTO_GENS)

dump_meta: dump_meta.cc $(META_PROTO_SRC)
	$(AM_V_CCLD) $(CXX) $(CXXFLAGS) -o $@ $^ $(COMMON_DEPS) $(LDFLAGS)

empty_trash: empty_trash.cc
	$(AM_V_CCLD) $(CXX) $(CXXFLAGS) -o $@ $^ $(COMMON_DEPS) $(LDFLAGS)

check_binlog_hole: check_binlog_hole.cc
	$(AM_V_CCLD) $(CXX) $(CXXFLAGS) -o $@ $^ $(COMMON_DEPS) $(LDFLAGS)

checknfix: checknfix.cc $(META_PROTO_SRC)
	$(AM_V_CCLD) $(CXX) $(CXXFLAGS) -o $@ $^ $(COMMON_DEPS) $(LDFLAGS)

binlog_reader: binlog_reader.cc $(NODE_PROTO_SRC) $(ZP_SRC_PATH)/common/zp_binlog.cc
	$(AM_V_CCLD) $(CXX) $(CXXFLAGS) -o $@ $^ $(COMMON_DEPS) $(LDFLAGS) -lglog

clean: 
	rm -rf ./*.o
	rm $(BINARIES)
