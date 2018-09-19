#!/bin/bash

cd $(dirname `readlink -f $BASH_SOURCE`)
source set-env.sh

cd ${HADOOP_CLIENT_EMULATOR}
mvn package

echo "finish build."
