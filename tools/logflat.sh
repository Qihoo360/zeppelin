#!/bin/sh
list_alldir(){
  mkdir "log_tmp"
  for f in `ls -a $1`
  do
    if [ x"$f" != x"." -a x"$f" != x".." ];then
      if [ ${f##*.} == "gz" ];then
        gunzip -c $f > log_tmp/${f%.*}
      fi

    fi
  done
}

list_alldir .
