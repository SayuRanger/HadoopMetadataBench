#/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`


cd $abshere
source set-env.sh

if (( $# == 0 ))
then
	for node in `cat ${HADOOP_CLIENT_EMULATOR_CONFIG}/node`
	do
		echo "connect to $node"
		ssh $node "$absme 1" 
	done
else
	echo `hostname`
	pkill -9 -f "java.*replayer" 
fi
#pkill sar
#pkill mem-cpu.sh
