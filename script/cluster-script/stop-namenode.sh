#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

cd ${abshere}

bin=${HADOOP_COMMON_HOME}/bin

nameStartOpt="$nameStartOpt $@"

#---------------------------------------------------------
# namenodes

NAMENODES=$($HADOOP_PREFIX/bin/hdfs getconf -namenodes)

echo "Stopping namenodes on [$HADOOP_MASTER_NODE]"

"$HADOOP_PREFIX/sbin/hadoop-daemon.sh" \
	  --config "$HADOOP_CONF_DIR" \
		  --hostnames "$HADOOP_MASTER_NODE" \
			  --script "$bin/hdfs" stop namenode $nameStartOpt

#---------------------------------------------------------

