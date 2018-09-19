#!/bin/bash

_absme=`readlink -f $BASH_SOURCE`
_abshere=`dirname $_absme`
_me=`basename $_absme`

[ -z ${HADOOP_CLIENT_EMULATOR} ] || exit 0

cd ${_abshere}

# folder on local disk
export LOCAL=/home/hadoop-shir/emulation

# HADOOP Configuration on test machine
export HADOOP_SHARE=${_abshere}/..
export HADOOP_SRC_HOME=${LOCAL}/hadoop273
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

# hadoop configure folder
export HADOOP_CONF_DIR=${HADOOP_SHARE}/hadoop-conf
export YARN_CONF_DIR=${HADOOP_CONF_DIR}

# hadoop log foler
export HADOOP_LOG_DIR=${LOCAL}/replayer-tmp/logs
export YARN_LOG_DIR=${HADOOP_LOG_DIR}
export YARN_ROOT_LOGGER="INFO,RFA"

# hadoop executable folder
export HADOOP_COMMON_HOME=${LOCAL}/hadoop-dist/hadoop-2.7.3
export HADOOP_PREFIX=${HADOOP_COMMON_HOME}
export HADOOP_HOME=${HADOOP_COMMON_HOME}
export HADOOP_HDFS_HOME=${HADOOP_COMMON_HOME}
export HADOOP_YARN_HOME=${HADOOP_COMMON_HOME}
export HADOOP_TMP_DIR=${LOCAL}/replayer-tmp/hadoop
export HADOOP_MAPRED_HOME=${HADOOP_COMMON_HOME}
export PATH=$HADOOP_PREFIX:$HADOOP_PREFIX/bin:$HADOOP_PREFIX/sbin:$PATH	
if [ -d ${HADOOP_PREFIX/bin} ]
then
	export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:$(hadoop classpath)
fi

# set HADOOP_MASTER_NODE to the machine that run the Hadoop NameNode
export HADOOP_MASTER_NODE=node220-3
export HADOOP_VERSION_CODE='2.7.3'
export HADOOP_CLIENT_EMULATOR=${HADOOP_SHARE}/emulator
export HADOOP_CLIENT_EMULATOR_CONFIG=${HADOOP_CLIENT_EMULATOR}/conf

# emulator log folder
export EMULATOR_LOG_DIR=${LOCAL}/replayer-tmp/emulator-log
# export HADOOP_PARTITION_NUM=30
export YARN_RESOURCEMANAGER_HEAPSIZE=10240

