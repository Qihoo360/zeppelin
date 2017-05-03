#!/bin/sh

###################
# Client.proto
###################
protoc -I=./ --cpp_out=./ ./client.proto

HEADER=client.pb.h
SRC=client.pb.cc

cp ${HEADER} ../include
cp ${SRC} ../src/node/
cp ${HEADER} ../test/
cp ${SRC} ../test/
rm ${HEADER}
rm ${SRC}


######################
## zp_data_control.proto
####################
#protoc -I=./ --cpp_out=./ zp_data_control.proto
#
#SERVER_HEADER=zp_data_control.pb.h
#SERVER_SRC=zp_data_control.pb.cc
#
#mv ${SERVER_HEADER} ../include
#mv ${SERVER_SRC} ../src/node/

#####################
# zp_meta.proto
###################
protoc -I=./ --cpp_out=./ zp_meta.proto

SERVER_HEADER=zp_meta.pb.h
SERVER_SRC=zp_meta.pb.cc

cp ${SERVER_HEADER} ../test/
mv ${SERVER_HEADER} ../include
cp ${SERVER_SRC} ../test/
mv ${SERVER_SRC} ../src/common/

