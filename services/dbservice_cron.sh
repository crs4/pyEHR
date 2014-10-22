#!/bin/bash

# This script can be used to handle the dbservice daemon as a cronjob
# add to the crontab a line like
#
# */5 * * * * $PYEHR_REPO_PATH/services/dbservice_cron.sh &

SERVICE_DIR=/opt/pyEHR/services
PID_FILE=/tmp/pyehr_dbservice.pid
SERVICE_CONF=/opt/pyEHR/conf/dbservice.conf
LOG_FILE=/tmp/pyehr_dbservice.log

nohup python ${SERVICE_DIR}/dbservice.py --config ${SERVICE_CONF} --pid-file ${PID_FILE} --log-file ${LOG_FILE}
