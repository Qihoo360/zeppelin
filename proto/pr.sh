#!/bin/sh

# Client.proto
protoc -I=./ --cpp_out=./ ./client.proto

HEADER=client.pb.h
SRC=client.pb.cc

cp ${HEADER} ../include
cp ${SRC} ../src
cp ${HEADER} ../sdk/
cp ${SRC} ../sdk
rm ${HEADER}
rm ${SRC}


# server_control.proto

protoc -I=./ --cpp_out=./ server_control.proto

SERVER_HEADER=server_control.pb.h
SERVER_SRC=server_control.pb.cc

cp ${SERVER_HEADER} ../include
cp ${SERVER_SRC} ../src
rm ${SERVER_HEADER}
rm ${SERVER_SRC}

