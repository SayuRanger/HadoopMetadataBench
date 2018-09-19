#!/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

ddd=`date +%m%d_%H%M`
cd $abshere

[ -z ${HADOOP_CLIENT_EMULATOR} ] && source set-env.sh

if [ $# -ne 1 ]
then
	echo "please run this script with workload name"
	echo "e.g."
	echo "$BASH_SOURCE nn-terasort-dn100-c1"
	echo "currently support nn-terasort-dn100-c1,nn-terasort-dn50-c8,rm-terasort-dn100-c1"
	exit 0
fi

WORK_LOAD=${HADOOP_CLIENT_EMULATOR_CONFIG}/${1}

[ ! -d ${WORK_LOAD}/workload ] && { echo "wrong worklaod name: ${1}"; exit -1; } 

# Set LAUNCH_NODE to the node that you want to launch the test
LAUNCH_NODE=node220-6
if [ `hostname` != ${LAUNCH_NODE} ]
then
	echo "now it is starting script on ${LAUNCH_NODE}"
	ssh ${LAUNCH_NODE} "$absme ${1}"
	exit $?
fi


if [ ! -f ${WORK_LOAD}/node ]
then
	echo "no such file nodes_j${1}"
	echo "exit."
	exit 1
fi

cat ${WORK_LOAD}/node > ${HADOOP_CLIENT_EMULATOR_CONFIG}/node

./kill-emulator.sh

cat ${WORK_LOAD}/joblist > ${HADOOP_CLIENT_EMULATOR_CONFIG}/joblist
cat ${WORK_LOAD}/conf > ${HADOOP_CLIENT_EMULATOR_CONFIG}/conf

WORK_TYPE=""
if [[ ${1} == nn* ]]
then
	ssh ${HADOOP_MASTER_NODE} "${HADOOP_SHARE}/script/cluster-script/reboot-namenode.sh"
	WORK_TYPE="nn"
elif [[ ${1} == rm* ]]
then
	ssh ${HADOOP_MASTER_NODE} "${HADOOP_SHARE}/script/cluster-script/reboot-yarn.sh"
	WORK_TYPE="rm"
else
	echo "worklaod name may be wrong. exit."
	exit -1
fi

for i in `cat ${HADOOP_CLIENT_EMULATOR_CONFIG}/node`
do
	ssh $i "${abshere}/launch-emulator.sh ${ddd} ${WORK_TYPE}" &
done

