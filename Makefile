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

ifeq ($(__REL), 1)
#CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
	CXXFLAGS = -O2 -g -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -gstabs+ -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
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
			   -I$(THIRD_PATH)/nemo/output/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/floyd/output/include/


LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/floyd/output/lib/ \
		   -L$(THIRD_PATH)/nemo/output/lib/ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/ \
		   -L$(THIRD_PATH)/glog/.libs/


LIBS = -lpthread \
	   -lprotobuf \
	   -lglog \
	   -lnemo \
	   -lfloyd \
	   -lslash \
	   -lleveldb \
	   -lrocksdb \
		 -lpink \
	   -lz \
	   -lbz2 \
	   -lsnappy \
	   -lrt

FLOYD = $(THIRD_PATH)/floyd/output/lib/libfloyd.a
NEMO = $(THIRD_PATH)/nemo/output/lib/libnemo.a
GLOG = $(THIRD_PATH)/glog/.libs/libglog.so.0
PINK = $(THIRD_PATH)/pink/output/lib/libpink.a
SLASH = $(THIRD_PATH)/slash/output/lib/libslash.a

.PHONY: all clean


all: $(ZP_META) $(ZP_NODE)
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
	#mkdir $(OUTPUT)/tools
	@echo "Success, go, go, go..."


$(ZP_META): $(FLOYD) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(META_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(META_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS) 

$(ZP_NODE): $(NEMO) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(NODE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(NODE_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS) 

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@  

$(FLOYD):
	make -C $(THIRD_PATH)/floyd/

$(NEMO):
	make -C $(THIRD_PATH)/nemo/

$(SLASH):
	make -C $(THIRD_PATH)/slash/

$(PINK):
	make -C $(THIRD_PATH)/pink/

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

