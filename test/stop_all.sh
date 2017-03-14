#!/bin/bash

#DIR=${1-:../}
#DIR=`dirname $0`
DIR=`pwd`

ps aux| grep zp- | awk '{print $2}' | xargs kill -9

if [ $1'x' = 'clean''x' ] ; then 
  echo "Clean files"
  rm -rf ${DIR}/meta* ${DIR}/node* ${DIR}/sync*
fi
