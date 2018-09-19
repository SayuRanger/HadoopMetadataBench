#/bin/bash

absme=`readlink -f ${BASH_SOURCE}`
abshere=`dirname ${absme}`
me=`basename $absme`

cd $abshere
source set-env.sh
mkdir -p tmp-log

if [ $# -eq 0 ]
then
	rm -r tmp-log/*
	cp ${HADOOP_CLIENT_EMULATOR_CONFIG}/conf tmp-log/
	cp ${HADOOP_CLIENT_EMULATOR_CONFIG}/joblist tmp-log/
	for node in `cat ${HADOOP_CLIENT_EMULATOR_CONFIG}/node`
	do
		ssh $node "$absme 1"
	done
else
	cd $abshere
	cp -r ${EMULATOR_LOG_DIR}/* tmp-log/
fi
