#/bin/bash

absme=`readlink -f $BASH_SOURCE`
abshere=`dirname $absme`
me=`basename $absme`

cd $abshere
source set-env.sh

if [ $# -eq 0 ]
then
	for node in `cat ${HADOOP_CLIENT_EMULATOR_CONFIG}/node`
	do
		echo "check log on $node"
		ssh $node "$absme 1"
	done
else
	tail $EMULATOR_LOG_DIR/sim*
	grep -r $EMULATOR_LOG_DIR/ -ie "exception"
fi
