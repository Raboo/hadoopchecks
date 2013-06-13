#!/usr/bin/env bash

OVERUSE=75
CHECK_INTVL=180

function WARN() {
	echo "`date "+%Y-%m-%d %H:%M:%S"` [WARN] ""$@"
}

function usage() {
cat << endl
Usage: $(basename "$0") 
	[-over OVERUSE]          #warn after hdfs usage goes beyond OVERUSE, in percent
	[-intvl CHECK_INTVL]   #check interval, in seconds
	[-help]
endl
}
while [ $# -gt 0 ]; do
	case "$1" in
		-help)
			usage; exit 0;;
		-over)
			shift;OVERUSE=$1;;
		-intvl)
			shift;CHECK_INTVL=$1;;
		*)
			echo "Uknown option $1"
			usage; exit -1;;
	esac
	shift
done

while [[ true ]]; do
	SUM=0
	if [ "${OVERUSE}" != "" ]; then
		for us in `hadoop dfsadmin -report | egrep "^DFS Used%: " | sed 's/DFS Used%: //' | egrep -o '^[0-9]+'`; do
			[ $us -ge $OVERUSE ] && SUM=$(( $SUM + 1))
		done
	fi
	DEADNODE=`hadoop dfsadmin -report | egrep "^Datanodes available: " | egrep -o "[0-9]+ dead" | awk '{print $1}'`
	[ "${SUM}" != "0" ] && WARN "${SUM} nodes are hdfs over-used by ${OVERUSE}"
	[ "${DEADNODE}" != "0" ] && WARN "${DEADNODE} nodes is dead"
	sleep ${CHECK_INTVL}
done

