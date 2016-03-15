#!/usr/bin/env bash

<<<ENDCOMMENT
# these setup routines are used to set up an environment when non is available, like in Vagrant
# since the script usually runs on the Telemetry Spark Analysis environment, the requirements should already be installed

# install packages
sudo apt-get install libsnappy-dev liblzma-dev python-psycopg2 openjdk-7-jdk
sudo pip2 install telemetry-tools
sudo apt-get install python-numpy python-pandas # these are dependencies of python_moztelemetry, but installing them with apt-get massively speeds up python_moztelemetry installation
sudo pip2 install python_moztelemetry

# install Spark
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.4.tgz -O spark-hadoop.tgz
tar -xzf spark-hadoop.tgz
rm spark-hadoop.tgz
export SPARK_HOME=$(pwd)/spark-hadoop
export PYTHONPATH=$SPARK_HOME/python
ENDCOMMENT

# get DB password from S3 (requires AWS credentials)
aws s3 cp s3://telemetry-spark-emr-2/crash_rate_aggregates_credentials crash_rate_aggregates_credentials
DB_PASS=$(python -c 'import json;print json.load(open("crash_rate_aggregates_credentials", "r"))["password"]')
rm crash_rate_aggregates_credentials

# fill crash aggregates
DB_NAME=crash_rate_aggregates
DB_USER=root
DB_HOST=crash-rate-aggregates.cppmil15uwvg.us-west-2.rds.amazonaws.com
python fill_database.py \
  --pg-host "$DB_HOST" --pg-name "$DB_NAME" \
  --pg-username "$DB_USER" --pg-password "$DB_PASS"
