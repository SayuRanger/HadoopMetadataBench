#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

cd ${abshere}/..
source set-env.sh
cd ${abshere}

[ -z ${YARN_LOG_DIR} ] && exit -1

./stop-yarn.sh
rm -rf ${YARN_LOG_DIR}/*
./start-yarn.sh

echo "yarn reboot finished."
