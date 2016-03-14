#!/usr/bin/env bash

# get DB password from S3 (requires AWS credentials)
aws s3 cp s3://telemetry-spark-emr-2/crash_rate_aggregates_credentials crash_rate_aggregates_credentials
DB_PASS=$(python -c 'import json;print json.load(open("crash_rate_aggregates_credentials", "r"))["password"]')
rm crash_rate_aggregates_credentials

# fill crash aggregates
DB_NAME=crash_rate_aggregates
DB_USER=root
DB_HOST=crash-rate-aggregates.cppmil15uwvg.us-west-2.rds.amazonaws.com
python fill_database.py \
  --aws-rdb-host "$DB_HOST" --aws-rdb-name "$DB_NAME" \
  --aws-rdb-username "$DB_USER" --aws-rdb-password "$DB_PASS"
