#!/bin/bash

# Change node220-4 to target node if needed
if [ `hostname` != node220-4 ]
then
	echo "error: please run this script on node220-4"
	exit 1
fi

LOCAL=/home/hadoop-shir/emulation
HADOOP_SRC_HOME=${LOCAL}/hadoop273
HADOOP_DIST=${LOCAL}/hadoop-dist

# create experiment folders
HADOOP_LOG_DIR=${LOCAL}/replayer-tmp/logs
HADOOP_TMP_DIR=${LOCAL}/replayer-tmp/hadoop
EMULATOR_LOG_DIR=${LOCAL}/replayer-tmp/emulator-log

for i in `seq 3 6`
do
	node="node220-$i"
    ssh $node "mkdir -p ${HADOOP_DIST} ${HADOOP_LOG_DIR} ${HADOOP_TMP_DIR} ${EMULATOR_LOG_DIR}; ls -l ${HADOOP_LOG_DIR}/../"
done

# Build hadoop 2.7.3 on target node
code=0
cd ${HADOOP_SRC_HOME}
mvn clean && mvn package -Pdist -DskipTests -Dtar -Dmaven.javadoc.skip=true && code=2 || code=-2

if ((code<0))
then
	echo "some error happened. code=$code"
	exit 5
fi

cd hadoop-dist/target/

# Copy build tarball to node220-[3-6], change range if needed
for i in `seq 3 6`
do
	node="node220-$i"
	echo $node
	scp hadoop-2.7.3.tar.gz $node:${HADOOP_DIST}
done

cd ${HADOOP_DIST}
# Unzip the build tarball to node220-[3-6]
for i in `seq 3 6`; do node="node220-$i"; echo $node; loc=${HADOOP_DIST}; ssh $node "cd $loc && rm -rf hadoop-2.7.3 && tar -zxf hadoop-2.7.3.tar.gz"; done

echo "build finish".

