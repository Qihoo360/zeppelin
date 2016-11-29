#!/bin/bash

#DIR=${1:-../}

DIR=`dirname $0`
#PWD=`pwd`
#echo "pwd is `pwd`"
echo "DIR is ${DIR}"
echo "Start Meta ..."
sh ${DIR}/start_meta.sh ${DIR}
#sh ${DIR}/start_meta.sh ${PWD}

sleep 3
echo "Start Data ..."
sh ${DIR}/start_data.sh ${DIR}

