#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

cd ${abshere}/..
source set-env.sh
cd ${abshere}

#---------------------------------------------------------
# namenodes
./stop-namenode.sh

[ -z ${HADOOP_LOG_DIR} ] && { echo "error, HADOOP_LOG_DIR not set"; exit -1; }

rm $HADOOP_LOG_DIR/*
rm -rf ${HADOOP_TMP_DIR}/dfs/
rm -rf ${HADOOP_LOG_DIR}/*

hadoop namenode -format

./start-namenode.sh
#---------------------------------------------------------
hdfs dfsadmin -safemode leave

