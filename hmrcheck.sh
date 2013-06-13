#!/usr/bin/env bash

HMRCHK_HOME=`dirname "$0"`
HMRCHK_HOME=`cd "$HMRCHK_HOME"; pwd`

CHKMARK=${HMRCHK_HOME}/.mrchk_mark
HEARTBEAT_TIMEOUT=$((60 * 60))

function WARN() {
	echo "`date "+%Y-%m-%d %H:%M:%S"` [WARN] ""$@"
}

function usage() {
cat << endl
Usage: $(basename "$0") 
	[-intvl HEARTBEAT_INTVL]         #interval of heart beat, in seconds
	[-timeout HEARTBEAT_TIMEOUT]     #how long should heartbeat deemed failure, in minutes
	[-help]
endl
}
while [ $# -gt 0 ]; do
	case "$1" in
		-help)
			usage; exit 0;;
		-intvl)
			shift;HEARTBEAT_INTVL=$1;;
		-timeout)
			shift;HEARTBEAT_TIMEOUT=$((${1} * 60));;
		*)
			echo "Uknown option $1"
			usage; exit -1;;
	esac
	shift
done

MAPRED_HEARTBEAT_SH=${HMRCHK_HOME}/.mapred_heartbeat_check.sh
#echo "${mapred_heartbeat}" > ${MAPRED_HEARTBEAT_SH}

cat <<ENDL > ${MAPRED_HEARTBEAT_SH}
#!/usr/bin/env bash
while [ true ]; do
	[ ! -e @CHKMARK@ ] && break
	@HUGETABLE_HOME@/bin/hadoop jar @HUGETABLE_HOME@/hadoop-examples-0.20.2-cdh3u4.jar sleep -m 1 -r 1 > /dev/null 2>&1
	errcode=\$?
	if [ "\$errcode" = "0" ]; then
		echo "OK \`date '+%s'\`" > @CHKMARK@
	else
		echo "ERROR \`date '+%s'\` \$errcode" > @CHKMARK@
	fi
	[ ! -e @CHKMARK@ ] && break
	sleep @HEARTBEAT_INTVL@
done
echo "Exit mapred heartbeat!"
ENDL

function unescape_bs() { #unescape back slash '/', specialy usefull for 'sed'
	echo "$@" | sed "s#/#\\\\/#g"
}

sed -i "s/@HUGETABLE_HOME@/$(unescape_bs "${HUGETABLE_HOME}")/g" ${MAPRED_HEARTBEAT_SH}
sed -i "s/@CHKMARK@/$(unescape_bs "${CHKMARK}")/g" ${MAPRED_HEARTBEAT_SH}
sed -i "s/@HEARTBEAT_INTVL@/${HEARTBEAT_INTVL:-$((60*20))}/g" ${MAPRED_HEARTBEAT_SH}

chmod +x ${MAPRED_HEARTBEAT_SH}
truncate -s 0 ${CHKMARK}

${MAPRED_HEARTBEAT_SH} &
#CHILDPID=$!

LAST_OK=$(date '+%s')
WARNING=no
while [ true ]; do
	[ ! -e ${CHKMARK} ] && break
	HBSTATUS=`cat ${CHKMARK}`
	[[ "${HBSTATUS}" =~ "ERROR" ]] && WARN "${HBSTATUS}"
	[[ "${HBSTATUS}" =~ "OK" ]] && LAST_OK=`echo "${HBSTATUS}" | awk '{print $2}'`
	NOW=$(date '+%s')
	ELAPSED=$((${NOW} - ${LAST_OK}))
	if [ ${ELAPSED} -gt ${HEARTBEAT_TIMEOUT} ]; then
		[ "${WARNING}" != "yes" ] && WARNING=yes && WARN "MAPRED heartbeat timeout after ${ELAPSED}s. Last successful `date -d @${LAST_OK}`."
		
	else
		[ "${WARNING}" != "no" ] && WARNING=no && WARN "MAPRED heartbeat restored."
	fi
	sleep 60
done

#kill ${CHILDPID}
echo "Exit mapred heartbeat check"
