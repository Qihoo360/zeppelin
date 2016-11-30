#!/bin/bash

DIR=${1-:./}
ZP_META=${DIR}/../output/bin/zp-meta
#ZP_DATA=${DIR}/output/bin/zp-data

CONF=${DIR}/conf_test/

nohup ${ZP_META} -c ${CONF}/meta1.conf &>meta1.log &
sleep 1;
nohup ${ZP_META} -c ${CONF}/meta2.conf &>meta2.log &
sleep 1;
nohup ${ZP_META} -c ${CONF}/meta3.conf &>meta3.log &
