#!/bin/bash

# This script can be used to handle the dbservice daemon as a cronjob
# add to the crontab a line like
#
# */5 * * * * $PYEHR_REPO_PATH/services/queryservice_cron.sh &

SERVICE_DIR=/opt/pyEHR/services
PID_FILE=/tmp/pyehr_queryservice.pid
SERVICE_CONF=/opt/pyEHR/conf/queryservice.conf
LOG_FILE=/tmp/pyehr_queryservice.log

nohup python ${SERVICE_DIR}/queryservice.py --config ${SERVICE_CONF} --pid-file ${PID_FILE} --log-file ${LOG_FILE}
