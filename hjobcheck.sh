#!/usr/bin/env bash

INVERT=false

MAX_RUN_TIME=60
CHECK_INTVL=180

function WARN() {
	echo "`date "+%Y-%m-%d %H:%M:%S"` [WARN] ""$@"
}

function debug() {
	[ "$DEBUG" = "yes" ] && echo "`date "+%Y-%m-%d %H:%M:%S"` [DEBUG] ""$@"
}

function usage() {
cat << endl
Usage: $(basename "$0") 
	[-start JOBID]     #start from this JOBID, e.g., job_201305201422_0085
	[-nokilled]        #do not warn killed jobs
	[-maxrun MAX_RUN_TIME] #warn job run longer than this, in minutes
	[-intvl CHECK_INTVL]   #check interval, in seconds
	[-debug]
	[-help]
endl
}

while [ $# -gt 0 ]; do
	case "$1" in
		-help)
			usage; exit 0;;
		-nokilled)
			EXCLUDE_KILLED=yes;;
		-debug)
			DEBUG=yes;;
		-start)
			shift;LASTJOBID=$1;;
		-maxrun)
			shift;MAX_RUN_TIME=$1;;
		-intvl)
			shift;CHECK_INTVL=$1;;
		*)
			echo "Uknown option $1"
			usage; exit -1;;
	esac
	shift
done

if [ "${EXCLUDE_KILLED}" = "yes" ]; then
	EXCLUDESTATUS=(succeeded successful running killed)
else
	EXCLUDESTATUS=(succeeded successful running)
fi

while [[ true ]]; do
	(debug "status checking after ($LASTJOBID)...")
	#check for job status
	if [ "${LASTJOBID}" = "" ]; then
		CHECKLIST=(`hadoop job -list all | grep "^job_" | awk '{print $1}' | sort | tail -n 1`)
		LASTJOBID=${CHECKLIST[0]}
	else
		CHECKLIST=(`hadoop job -list all | grep "^job_" | awk '{print $1}' | sort | sed -e "1,/${LASTJOBID}/d"`)
	fi
	if [ ${#CHECKLIST[@]} -gt 0 ]; then
		freeze=no
		for jobid in ${CHECKLIST[@]}; do
			joburl=`hadoop job -status ${jobid} | grep "^tracking URL: " | sed 's/tracking URL: //'`
			status=`curl ${joburl} 2>/dev/null | egrep -o "<b>Status:</b> (\w+)" | awk '{print $2}'`
			status=`echo $status | tr '[A-Z]' '[a-z]'`
			(debug "job $jobid url $joburl $status")
			[ "${status}" = "" ] && WARN "Cannot get status for job ${jobid}" && continue
			[[ "${status}" =~ "running" ]] && freeze="yes"
			[[ "${freeze}" != "yes" ]] && LASTJOBID=${jobid}
			[[ ! "${EXCLUDESTATUS[@]}" =~ "${status}" ]] && WARN "Job ${jobid} is ${status}"
		done
	fi
	#check for over-run-time jobs
	CHECKLIST=(`hadoop job -list | grep "^job_" | awk '{print $1}'`)
	if [ ${#CHECKLIST[@]} -gt 0 ]; then
		for jobid in ${CHECKLIST[@]}; do
			joburl=`hadoop job -status ${jobid} | grep "^tracking URL: " | sed 's/tracking URL: //'`
			RUNNINGFOR=`curl ${joburl} 2>/dev/null | egrep -o "<b>Running for:</b> (.*)"`
			[ "$RUNNINGFOR" = "" ] && continue
			hrs=`echo "${RUNNINGFOR}" | egrep -o "[0-9]+hrs," | egrep -o "[0-9]+"`
			mins=`echo "${RUNNINGFOR}" | egrep -o "[0-9]+mins," | egrep -o "[0-9]+"`
			minutes=$((${hrs:-0} * 60 + ${mins:-0}))
			[ $minutes -ge $MAX_RUN_TIME ] && WARN "Job $jobid has running for ${minutes} minutes"
		done
	fi
	sleep ${CHECK_INTVL}
done
