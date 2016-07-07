RPATH = /usr/local/zeppelin/lib/
LFLAGS = -Wl,-rpath=$(RPATH)

UNAME := $(shell if [ -f "/etc/redhat-release" ]; then echo "CentOS"; else echo "Ubuntu"; fi)

OSVERSION := $(shell cat /etc/redhat-release | cut -d "." -f 1 | awk '{print $$NF}')

ifeq ($(UNAME), Ubuntu)
  SO_DIR = $(CURDIR)/lib/ubuntu
else ifeq ($(OSVERSION), 5)
  SO_DIR = $(CURDIR)/lib/5.4
else
  SO_DIR = $(CURDIR)/lib/6.2
endif

CXX = g++

ifeq ($(__REL), 1)
#CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -fPIC -Wno-unused-function -std=c++11
	CXXFLAGS = -O2 -g -pipe -fPIC -W -DNDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
else
	CXXFLAGS = -O0 -g -gstabs+ -pg -pipe -fPIC -W -DDEBUG -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -Wno-unused-parameter -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -Wno-redundant-decls
endif

SRC_DIR = ./src
THIRD_PATH = ./third
OUTPUT = ./output


BASE_OBJS = client.pb.o zp_binlog.o zp_pb_cli.o \
			 server_control.pb.o zp_command.o\
			  zp_admin.o  zp_options.o
COMMON_OBJS = $(patsubst %.o,$(SRC_DIR)/%.o,$(BASE_OBJS))

ZP_META = zp-meta
ZP_META_BASE_OBJS = zp_heartbeat_conn.o zp_heartbeat_thread.o\
							 			zp_meta.o zp_meta_server.o
ZP_META_OBJS = $(patsubst %.o,$(SRC_DIR)/%.o,$(ZP_META_BASE_OBJS))

ZP_NODE = zp-node
ZP_NODE_BASE_OBJS = zp_ping_thread.o zp_binlog_receiver_thread.o  zp_worker_thread.o\
							 zp_binlog_sender_thread.o zp_dispatch_thread.o\
							 zp_client_conn.o  zp_kv.o zp_sync_conn.o\
							 zp_node.o zp_data_server.o
ZP_NODE_OBJS = $(patsubst %.o,$(SRC_DIR)/%.o,$(ZP_NODE_BASE_OBJS))

OBJS = $(COMMON_OBJS) $(ZP_META_OBJS) $(ZP_NODE_OBJS) 


INCLUDE_PATH = -I./include/ \
			   -I./src/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/nemo/output/include/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/

LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/nemo/output/lib/ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/ \
		   -L$(THIRD_PATH)/glog/.libs/


LIBS = -lpthread \
	   -lprotobuf \
	   -lglog \
	   -lnemo \
	   -lslash \
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
	@echo "ZP_META_OBJS $(ZP_META_OBJS)"
	@echo "ZP_NODE_OBJS $(ZP_NODE_OBJS)"
	@echo "OBJS $(OBJS)"
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/bin
	#cp -r ./conf $(OUTPUT)/
	mkdir $(OUTPUT)/lib
	cp -r $(SO_DIR)/*  $(OUTPUT)/lib
	mv $(ZP_META) $(OUTPUT)/bin/
	mv $(ZP_NODE) $(OUTPUT)/bin/
	#mkdir $(OUTPUT)/tools
	@echo "Success, go, go, go..."


$(ZP_META): $(NEMO) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(ZP_META_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(ZP_META_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS) 

$(ZP_NODE): $(NEMO) $(GLOG) $(PINK) $(SLASH) $(COMMON_OBJS) $(ZP_NODE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $(COMMON_OBJS) $(ZP_NODE_OBJS) $(INCLUDE_PATH) $(LIB_PATH) $(LFLAGS) $(LIBS) 

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDE_PATH) -c $< -o $@  

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
		autoreconf -ivf; ./configure; make; echo '*' > $(CURDIR)/third/glog/.gitignore; cp $(CURDIR)/third/glog/.libs/libglog.so.0 $(SO_DIR); \
	fi; 
	
clean: 
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)

