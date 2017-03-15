RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

UNAME := $(shell if [ -f "/etc/redhat-release" ]; then echo "CentOS"; else echo "Ubuntu"; fi)

OSVERSION := $(shell cat /etc/redhat-release | cut -d "." -f 1 | awk '{print $$NF}')

ifeq ($(UNAME), Ubuntu)
  SO_PATH = $(CURDIR)/lib/ubuntu
else ifeq ($(OSVERSION), 5)
  SO_PATH = $(CURDIR)/lib/5.4
else
  SO_PATH = $(CURDIR)/lib/6.2
endif

CXX = g++

ifeq ($(__PERF), 1)
#CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
	CXXFLAGS = -O0 -g -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
else
	CXXFLAGS = -O2 -g -gstabs+ -pg -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls -DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX -DOS_LINUX
endif

COMMON_SRC_PATH = ./src/common
META_SRC_PATH = ./src/meta
NODE_SRC_PATH = ./src/node
THIRD_PATH = ./third
OUTPUT = ./output


COMMON_SRC = $(wildcard $(COMMON_SRC_PATH)/*.cc)
COMMON_OBJS = $(patsubst %.cc,%.o,$(COMMON_SRC))


META_SRC = $(wildcard $(META_SRC_PATH)/*.cc)
META_OBJS = $(patsubst %.cc,%.o,$(META_SRC))

NODE_SRC = $(wildcard $(NODE_SRC_PATH)/*.cc)
NODE_OBJS = $(patsubst %.cc,%.o,$(NODE_SRC))


ZP_META = zp-meta

ZP_NODE = zp-node

OBJS = $(COMMON_OBJS) $(META_OBJS) $(NODE_OBJS) 


INCLUDE_PATH = -I./include/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/nemo-rocksdb/ \
			   -I$(THIRD_PATH)/nemo-rocksdb/rocksdb \
			   -I$(THIRD_PATH)/nemo-rocksdb/rocksdb/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/pink/output/ \
			   -I$(THIRD_PATH)/floyd/output/include/


LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/floyd/output/lib/ \
		   -L$(THIRD_PATH)/nemo-rocksdb/output/lib/ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/ \
		   -L$(THIRD_PATH)/glog/.libs/


LIBS = -lpthread \
	   -lprotobuf \
	   -lglog \
	   -lslash \
		 -lpink \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

METALIBS = -lfloyd \
     -lleveldb

NODELIBS = -lnemodb \
     -lrocksdb

FLOYD = $(THIRD_PATH)/floyd/output/lib/libfloyd.a
NEMODB = $(THIRD_PATH)/nemo-rocksdb/output/lib/libnemodb.a
GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0
PINK = $(THIRD_PATH)/pink/output/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/output/lib/libslash.a

.PHONY: all clean distclean


all: $(ZP_META) $(ZP_NODE)
#all: $(ZP_NODE)
#all: 
	@echo "COMMON_OBJS $(COMMON_OBJS)"
	@echo "ZP_META_OBJS $(META_OBJS)"
	@echo "ZP_NODE_OBJS $(NODE_OBJS)"
	@echo "OBJS $(OBJS)"
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	#cp -r ./conf $(OUTPUT)/
	mkdir $(OUTPUT)/lib
	cp -r $(SO_PATH)/*  $(OUTPUT)/lib
	mv $(ZP_META) $(OUTPUT)/bin/
	mv $(ZP_NODE) $(OUTPUT)/bin/
	cp -r conf $(OUTPUT)
	#mkdir $(OUTPUT)/tools
	@echo "Success, go, go, go..."


$(ZP_META): $(FLOYD) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(META_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(META_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(METALIBS) $(LIBS) 

$(ZP_NODE): $(NEMODB) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(NODE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(NODE_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(NODELIBS) $(LIBS) 

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@  

$(FLOYD):
	make -C $(THIRD_PATH)/floyd/ __PERF=$(__PERF)

$(NEMODB):
	make -C $(THIRD_PATH)/nemo-rocksdb/

$(SLASH):
	make -C $(THIRD_PATH)/slash/ __PERF=$(__PERF)

$(PINK):
	make -C $(THIRD_PATH)/pink/ __PERF=$(__PERF)

$(GLOG):
	#if [ -d $(THIRD_PATH)/glog/.libs ]; then 
	if [ ! -f $(GLOG) ]; then \
		cd $(THIRD_PATH)/glog; \
		autoreconf -ivf; ./configure; make; echo '*' > $(CURPATH)/third/glog/.gitignore; cp $(CURPATH)/third/glog/.libs/libglog.so.0 $(SO_PATH); \
	fi; 
	
clean: 
	rm -rf $(COMMON_SRC_PATH)/*.o
	rm -rf $(META_SRC_PATH)/*.o
	rm -rf $(NODE_SRC_PATH)/*.o
	rm -rf $(OUTPUT)

distclean: clean
	make -C $(THIRD_PATH)/pink/ clean
	make -C $(THIRD_PATH)/slash/ clean
	make -C $(THIRD_PATH)/nemo-rocksdb/ clean
	make -C $(THIRD_PATH)/floyd/ distclean

