#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

cd ${abshere}

echo "starting yarn daemons"
bin=${HADOOP_COMMON_HOME}/sbin


DEFAULT_LIBEXEC_DIR="$bin"/../libexec
HADOOP_LIBEXEC_DIR=${HADOOP_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
$HADOOP_LIBEXEC_DIR/yarn-config.sh

# start resourceManager
"$bin"/yarn-daemon.sh --config $YARN_CONF_DIR start resourcemanager
