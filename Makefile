CXX= g++
LDFLAGS= -lpthread -lglog -lz -lbz2 -lsnappy -lrt
CXXFLAGS= -g -std=c++11 -fno-builtin-memcmp -msse -msse4.2 
PROFILING_FLAGS= -pg
OPT=

# DEBUG_LEVEL can have two values:
# * DEBUG_LEVEL=2; this is the ultimate debug mode. It will compile zeppelin
# without any optimizations. To compile with level 2, issue `make dbg`
# * DEBUG_LEVEL=0; this is the debug level we use for release. If you're
# running zeppelin in production you most definitely want to compile zeppelin
# with debug level 0. To compile with level 0, run `make`,

# Set the default DEBUG_LEVEL to 0
DEBUG_LEVEL?=0

ifeq ($(MAKECMDGOALS),dbg)
  DEBUG_LEVEL=2
endif

# compile with -O2 if debug level is not 2
ifneq ($(DEBUG_LEVEL), 2)
OPT += -O2 -fno-omit-frame-pointer
# if we're compiling for release, compile without debug code (-DNDEBUG) and
# don't treat warnings as errors
OPT += -DNDEBUG
DISABLE_WARNING_AS_ERROR=1
# Skip for archs that don't support -momit-leaf-frame-pointer
ifeq (,$(shell $(CXX) -fsyntax-only -momit-leaf-frame-pointer -xc /dev/null 2>&1))
OPT += -momit-leaf-frame-pointer
endif
else
$(warning Warning: Compiling in debug mode. Don't use the resulting binary in production)
OPT += $(PROFILING_FLAGS)
DEBUG_SUFFIX = "_debug"
endif

# ----------------------------------------------
OUTPUT = $(CURDIR)/output
THIRD_PATH = $(CURDIR)/third
SRC_PATH = $(CURDIR)/src
OPT += -D_GITVER_=$(shell git rev-list HEAD | head -n1)
OPT += -D_COMPILEDATE_=$(shell date +%F)
PROCESS_NUM=$(shell cat /proc/cpuinfo | grep "processor" | wc -l)
# ----------------Dependences-------------------

ifndef SLASH_PATH
SLASH_PATH = $(THIRD_PATH)/slash
endif
LIBSLASH = $(SLASH_PATH)/slash/lib/libslash$(DEBUG_SUFFIX).a

ifndef PINK_PATH
PINK_PATH = $(THIRD_PATH)/pink
endif
LIBPINK = $(PINK_PATH)/pink/lib/libpink$(DEBUG_SUFFIX).a

ifndef FLOYD_PATH
FLOYD_PATH = $(THIRD_PATH)/floyd
endif
LIBFLOYD = $(FLOYD_PATH)/floyd/lib/libfloyd$(DEBUG_SUFFIX).a

ifndef ROCKSDB_PATH
ROCKSDB_PATH = $(THIRD_PATH)/rocksdb
endif
LIBROCKSDB = $(ROCKSDB_PATH)/librocksdb$(DEBUG_SUFFIX).a

ifndef NEMODB_PATH
NEMODB_PATH = $(THIRD_PATH)/nemo-rocksdb
endif
LIBNEMODB = $(NEMODB_PATH)/lib/libnemodb$(DEBUG_SUFFIX).a

PROTODIR = $(THIRD_PATH)/protobuf
LIBPROTOBUF = $(PROTODIR)/_install/lib/libprotobuf.a
PROTOC = $(PROTODIR)/_install/bin/protoc

INCLUDE_PATH = -I. -I$(SLASH_PATH) -I$(PINK_PATH) -I$(FLOYD_PATH) \
							 -I$(NEMODB_PATH) -I$(ROCKSDB_PATH)/include \
							 -I$(PROTODIR)/_install/include

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

AM_LINK = $(AM_V_CCLD)$(CXX) $^ -o $@ $(LDFLAGS)
#-----------------------------------------------

# This (the first rule) must depend on "all".
default: all

WARNING_FLAGS = -W -Wextra -Wall -Wsign-compare \
  -Wno-unused-parameter -Wno-redundant-decls -Wwrite-strings \
	-Wpointer-arith -Wreorder -Wswitch -Wsign-promo \
	-Woverloaded-virtual -Wnon-virtual-dtor -Wno-missing-field-initializers

ifndef DISABLE_WARNING_AS_ERROR
  WARNING_FLAGS += -Werror
endif

CXXFLAGS += $(WARNING_FLAGS) $(INCLUDE_PATH) $(OPT)

COMMON_SRC = $(wildcard $(SRC_PATH)/common/*.cc)
COMMON_OBJS = $(patsubst %.cc,%.o,$(COMMON_SRC))

META_PROTO = $(wildcard $(SRC_PATH)/meta/*.proto)
META_PROTO_GENS = $(META_PROTO:%.proto=%.pb.cc) $(META_PROTO:%.proto=%.pb.h)
META_PROTO_OBJ = $(META_PROTO:%.proto=%.pb.o)
META_SRC = $(wildcard $(SRC_PATH)/meta/*.cc)
META_OBJS = $(patsubst %.cc,%.o,$(META_SRC))

NODE_PROTO = $(wildcard $(SRC_PATH)/node/*.proto)
NODE_PROTO_GENS = $(NODE_PROTO:%.proto=%.pb.cc) $(NODE_PROTO:%.proto=%.pb.h)
NODE_PROTO_OBJ = $(NODE_PROTO:%.proto=%.pb.o)
NODE_SRC = $(wildcard $(SRC_PATH)/node/*.cc)
NODE_OBJS = $(patsubst %.cc,%.o,$(NODE_SRC))

ZP_META = zp-meta$(DEBUG_SUFFIX)
ZP_NODE = zp-node$(DEBUG_SUFFIX)

.PHONY: distclean clean dbg all proto_gens

%.pb.cc %.pb.h: %.proto $(PROTOC)
	$(AM_V_GEN)
	$(AM_V_at)$(PROTOC) -I$(dir $<) --cpp_out=$(dir $<) $<

%.o: %.cc
	$(AM_V_CC)$(CXX) $(CXXFLAGS) -c $< -o $@

all: $(ZP_META) $(ZP_NODE)
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)mkdir $(OUTPUT)
	$(AM_V_at)mkdir $(OUTPUT)/bin
	$(AM_V_at)cp -r conf $(OUTPUT)
	$(AM_V_at)mv $(ZP_META) $(OUTPUT)/bin
	$(AM_V_at)mv $(ZP_NODE) $(OUTPUT)/bin

dbg: $(ZP_META) $(ZP_NODE)

proto: $(META_PROTO_GENS) $(NODE_PROTO_GENS)

$(ZP_META): $(META_PROTO_OBJ) $(NODE_PROTO_OBJ) $(COMMON_OBJS) $(META_OBJS) \
				$(LIBFLOYD) $(LIBPINK) $(LIBSLASH) $(LIBROCKSDB) $(LIBPROTOBUF)
	$(AM_V_at)rm -f $@
	$(AM_V_at)$(AM_LINK)

$(ZP_NODE): $(META_PROTO_OBJ) $(NODE_PROTO_OBJ) $(COMMON_OBJS) $(NODE_OBJS) \
				$(LIBNEMODB) $(LIBPINK) $(LIBSLASH) $(LIBROCKSDB) $(LIBPROTOBUF)
	$(AM_V_at)rm -f $@
	$(AM_V_at)$(AM_LINK)

$(LIBSLASH):
	$(AM_V_at)make -C $(SLASH_PATH)/slash DEBUG_LEVEL=$(DEBUG_LEVEL)

$(LIBPINK):
	$(AM_V_at)make -C $(PINK_PATH)/pink DEBUG_LEVEL=$(DEBUG_LEVEL) SLASH_PATH=$(SLASH_PATH)

$(LIBROCKSDB):
	$(AM_V_at)make -j $(PROCESSOR_NUMS) -C $(ROCKSDB_PATH) static_lib DISABLE_JEMALLOC=1 DEBUG_LEVEL=$(DEBUG_LEVEL)

$(LIBNEMODB):
	$(AM_V_at)make -C $(NEMODB_PATH) ROCKSDB_PATH=$(ROCKSDB_PATH) DEBUG_LEVEL=$(DEBUG_LEVEL)

$(LIBFLOYD):
	$(AM_V_at)make -C $(FLOYD_PATH)/floyd DEBUG_LEVEL=$(DEBUG_LEVEL) \
					ROCKSDB_PATH=$(ROCKSDB_PATH) SLASH_PATH=$(SLASH_PATH) PINK_PATH=$(PINK_PATH)

$(LIBPROTOBUF) $(PROTOC):
	cd $(PROTODIR); autoreconf -if; ./configure --prefix=$(PROTODIR)/_install --disable-shared; make install; echo '*' > $(PROTODIR)/.gitignore

clean:
	$(AM_V_at)echo "Cleaning"
	$(AM_V_at)rm -rf $(OUTPUT)
	$(AM_V_at)rm -f $(ZP_META) $(ZP_NODE)
	$(AM_V_at)rm -f $(META_PROTO_GENS) $(NODE_PROTO_GENS)
	$(AM_V_at)find $(SRC_PATH) -name "*.[oda]*" -exec rm -f {} \;
	$(AM_V_at)find $(SRC_PATH) -type f -regex ".*\.\(\(gcda\)\|\(gcno\)\)" -exec rm {} \;

distclean: clean
	$(AM_V_at)echo "Cleaning all"
	$(AM_V_at)make -C $(FLOYD_PATH)/floyd clean
	$(AM_V_at)make -C $(PINK_PATH)/pink clean
	$(AM_V_at)make -C $(SLASH_PATH)/slash clean
	$(AM_V_at)make -C $(NEMODB_PATH) clean
	$(AM_V_at)make -C $(ROCKSDB_PATH) clean
	$(AM_V_at)make -C $(PROTODIR) clean
