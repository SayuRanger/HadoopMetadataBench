#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

#log=${abshere}/log_dn3_`date +%Y%m%d_%H%M%S`
#mkdir -p ${log}
cd ${abshere}
source set-env.sh

cd ${HADOOP_CLIENT_EMULATOR}

# Note: make sure to install json-20160810.jar and change its path in classpath if needed
classpath=${HADOOP_CLASSPATH}:${HADOOP_CLIENT_EMULATOR}/target/ClientEmulator-1.0-SNAPSHOT.jar:~/.m2/repository/org/json/json/20160810/json-20160810.jar

echo "`hostname` start"

SIM_PARAM="-Xmx14336m -XX:ParallelGCThreads=8 -XX:+UseConcMarkSweepGC -Xss768k -cp ${classpath}"

[ -z ${EMULATOR_LOG_DIR} ] && { echo "EMULATOR_LOG_DIR not set, exit."; exit -1; }
[ ! -d ${EMULATOR_LOG_DIR} ] && { echo "no ${EMULATOR_LOG_DIR}, so create it now." ; mkdir -p ${EMULATOR_LOG_DIR}; } || { rm -rf ${EMULATOR_LOG_DIR}/*; }

if [ `hostname` = node220-4 ]
then
	#java ${SIM_PARAM} -agentlib:hprof=heap=sites,file=/tmp/prof.txt ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.0 &> ${EMULATOR_LOG_DIR}/sim0.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.0  0 &> ${EMULATOR_LOG_DIR}/sim0.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.1 11000 &> ${EMULATOR_LOG_DIR}/sim1.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.2 22000 &> ${EMULATOR_LOG_DIR}/sim2.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.3 33000 &> ${EMULATOR_LOG_DIR}/sim3.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.4 44000 &> ${EMULATOR_LOG_DIR}/sim4.log.${1} &
elif [ `hostname` = node220-5 ]
then
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.5 0 &> ${EMULATOR_LOG_DIR}/sim5.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.6 11000 &> ${EMULATOR_LOG_DIR}/sim6.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.7 22000 &> ${EMULATOR_LOG_DIR}/sim7.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.8 33000 &> ${EMULATOR_LOG_DIR}/sim8.log.${1} &
	java ${SIM_PARAM} ganyi.hadoop.replayer.Main SimulatorPool conf/conf simpool.9 44000 &> ${EMULATOR_LOG_DIR}/sim9.log.${1} &
fi


if [ `hostname` = node220-4 ]
then
	sleep 5
	echo "start centrol controller on `hostname`"
	java -cp ${classpath} ganyi.hadoop.replayer.Main CentralController conf/conf CC.1 conf/joblist ${2} &> ${EMULATOR_LOG_DIR}/CC.log.${1} &
fi

echo "`hostname` finish."
