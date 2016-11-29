#!/bin/bash

DIR=${1:-./}
#ZP_META=${DIR}/output/bin/zp-meta
ZP_DATA=${DIR}/../output/bin/zp-node

CONF=${DIR}/conf_test/

echo "Data pwd : " `pwd`
#nohup ${ZP_META} -c ${CONF}/meta1.conf &>meta1.log &
#nohup ${ZP_META} -c ${CONF}/meta2.conf &>meta2.log &
#nohup ${ZP_META} -c ${CONF}/meta3.conf &>meta3.log &

nohup ${ZP_DATA} -c ${CONF}/node1.conf > node1.log 2>&1 &
nohup ${ZP_DATA} -c ${CONF}/node2.conf > node2.log 2>&1 &
nohup ${ZP_DATA} -c ${CONF}/node3.conf > node3.log 2>&1 &
