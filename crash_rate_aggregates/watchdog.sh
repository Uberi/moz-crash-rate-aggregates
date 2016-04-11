#!/usr/bin/env bash

# watchdog timer for crash aggregator - if the aggregator job doesn't successfully complete within 10 hours, send out warning emails to telemetry-alerts@mozilla.com
# successful completion is marked by the existance of the file `crash_aggregates/JOB_SUCCESS`
# intended to be used with run_crash_aggregator.ipynb

SCRIPT_DIR=$(dirname "$(readlink -f -- "$0")") # get the directory this script is in

subject='[TIMEOUT] Crash rate aggregates job'
body='
Crash rate aggregates job failed to complete within 10 hours. Check logs at https://analysis.telemetry.mozilla.org/cluster/schedule for details.

This is an automatically generated message from the crash aggregates watchdog. The source code is available at https://github.com/mozilla/moz-crash-rate-aggregates.
'

sleep 36000 # 10 hours * 60 minutes/hours * 60 seconds/minutes = 36000 seconds
if [ ! -f "$SCRIPT_DIR/JOB_SUCCESS" ]; then
  aws ses send-email --from telemetry-alerts@mozilla.com --to telemetry-alerts@mozilla.com --text "$body" --subject "$subject"
fi
