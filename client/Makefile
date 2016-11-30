#protoc --proto_path=$(PRO_DIR) --cpp_out=$(PRO_DIR) $(PRO_DIR)/*.proto && mv $(PRO_DIR)/*.h  $(INC_DIR) && mv $(PRO_DIR)/*.cc $(SRC_DIR)
CXX = g++
#ifeq ($(__PERF), 1)
	CXXFLAGS = -O0 -g -pg -pipe -fPIC -D__XDEBUG__ -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -std=c++11 -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls
#else
#	CXXFLAGS = -O2 -g -pipe -fPIC -W -Wwrite-strings -Wpointer-arith -Wreorder -Wswitch -Wsign-promo -Wredundant-decls -Wformat -Wall -D_GNU_SOURCE -D__STDC_FORMAT_MACROS -std=c++11 -gdwarf-2 -Wno-redundant-decls -Wno-sign-compare
	# CXXFLAGS = -Wall -W -DDEBUG -g -O0 -D__XDEBUG__ -D__STDC_FORMAT_MACROS -fPIC -std=c++11 -gdwarf-2
#endif
THIRD_PATH = ../third
SRC_DIR := ./src/
INC_DIR := ./include/
OUTPUT := ./output/
PB := $(shell sh -c 'protoc --proto_path=$(PRO_DIR) --cpp_out=$(PRO_DIR) $(PRO_DIR)/*.proto && mv $(PRO_DIR)/*.h  $(INC_DIR) && mv $(PRO_DIR)/*.cc $(SRC_DIR)')


INCLUDE_PATH = -I./ \
			   -I./include/ \
			   -I$(THIRD_PATH)/glog/src/ \
			   -I$(THIRD_PATH)/slash/output/include/ \
			   -I$(THIRD_PATH)/pink/output/include/ \
			   -I$(THIRD_PATH)/pink/output/ 


LIB_PATH = -L./ \
		   -L$(THIRD_PATH)/slash/output/lib/ \
		   -L$(THIRD_PATH)/pink/output/lib/ 



LIBS = 

LIBRARY = libzp.a


.PHONY: all clean


BASE_OBJS := $(wildcard $(SRC_DIR)/*.cc)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.c)
BASE_OBJS += $(wildcard $(SRC_DIR)/*.cpp)
OBJS = $(patsubst %.cc,%.o,$(BASE_OBJS))


all: $(LIBRARY)
	make -C third/linenoise
	@echo "Success, go, go, go..."

$(LIBRARY): $(OBJS)
	rm -rf $(OUTPUT)
	mkdir $(OUTPUT)
	mkdir $(OUTPUT)/include
	mkdir $(OUTPUT)/lib
	rm -rf $@
	ar -rcs $@ $(OBJS)
	cp -r ./include $(OUTPUT)/
	mv $@ $(OUTPUT)/lib/
	#make -C hizeppelin


$(OBJECT): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(INCLUDE_PATH) $(LIB_PATH) -Wl,-Bdynamic $(LIBS)

$(OBJS): %.o : %.cc
	$(CXX) $(CXXFLAGS) -c $< -o $@ $(INCLUDE_PATH) 

clean: 
	make -C hizeppelin clean
	rm -rf $(SRC_DIR)/*.o
	rm -rf $(OUTPUT)/*
	rm -rf $(OUTPUT)
