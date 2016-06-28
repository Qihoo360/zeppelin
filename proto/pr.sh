#!/bin/sh
protoc -I=./ --cpp_out=./ ./client.proto

HEADER=client.pb.h
SRC=client.pb.cc

cp ${HEADER} ../include
cp ${SRC} ../src
cp ${HEADER} ../sdk/
cp ${SRC} ../sdk
rm ${HEADER}
rm ${SRC}

