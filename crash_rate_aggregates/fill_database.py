#!/usr/bin/env python

import re
import sys
from datetime import datetime, date, timedelta
import dateutil.parser

import numpy as np

from moztelemetry.spark import get_pings, get_pings_properties

import pyspark.sql.types as types

FRACTION = 1.0

# paths/dimensions within the ping to compare by, in the same format as the second parameter to `get_pings_properties`
# in https://github.com/mozilla/python_moztelemetry/blob/master/moztelemetry/spark.py
COMPARABLE_DIMENSIONS = [
    "environment/build/version",
    "environment/build/buildId",
    "application/channel",
    "application/name",
    "environment/system/os/name",
    "environment/system/os/version",
    "environment/build/architecture",
    "meta/geoCountry",
    "environment/addons/activeExperiment/id",
    "environment/addons/activeExperiment/branch",
    "environment/settings/e10sEnabled",
]

# names of the comparable dimensions above, used as dimension names in the database
DIMENSION_NAMES = [
    "build_version",
    "build_date",
    "channel",
    "application",
    "os_name",
    "os_version",
    "architecture",
    "country",
    "experiment_id",
    "experiment_branch",
    "e10s_enabled",
]
assert len(COMPARABLE_DIMENSIONS) == len(DIMENSION_NAMES)

INSERT_CHUNK_SIZE = 500 # number of records to accumulate in a single database request; higher values mean faster database insertion at the expense of memory usage

def compare_crashes(pings, start_date, end_date, comparable_dimensions, dimension_names):
    """Returns a PairRDD where keys are user configurations and values are Numpy arrays of the form [usage hours, main process crashes, content process crashes, plugin crashes]"""
    ping_properties = get_pings_properties(pings, comparable_dimensions + [
        "meta/submissionDate",
        "creationDate",
        "payload/info/subsessionLength",
        "meta/docType",
        "payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/content",
        "payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/plugin",
        "payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/gmplugin",
    ], with_processes=True)
    def is_valid(ping): # sanity check to make sure the ping is actually usable for our purposes
        submission_date = ping["meta/submissionDate"]
        if not isinstance(submission_date, str) and not isinstance(submission_date, unicode):
            return False
        activity_date = ping["creationDate"]
        if not isinstance(activity_date, str) and not isinstance(activity_date, unicode):
            return False
        subsession_length = ping["payload/info/subsessionLength"]
        if subsession_length is not None and not isinstance(subsession_length, int) and not isinstance(subsession_length, long):
            return False
        return True

    def get_crash_pair(ping): # responsible for normalizing a single ping into a crash pair
        # we need to parse and normalize the dates here rather than at the aggregates level,
        # because we need to normalize and get rid of the time portion

        # date the ping was received
        submission_date = datetime.strptime(ping["meta/submissionDate"], "%Y%m%d").date() # convert the YYYYMMDD format to a real date
        submission_date = max(start_date, min(end_date, submission_date)) # normalize the submission date if it's out of range

        # date the ping was created on the client
        try:
            activity_date = dateutil.parser.parse(ping["creationDate"]).date() # the activity date is the date portion of creationDate
        except: # malformed date, like how sometimes seconds that are not in [0, 59]
            activity_date = submission_date # we'll just set it to the submission date, since we can't parse the activity date
        activity_date = max(submission_date - timedelta(days=7), min(submission_date, activity_date)) # normalize the activity date if it's out of range

        return (
            # the keys we want to filter based on
            (submission_date, activity_date) + tuple(ping[key] for key in comparable_dimensions), # all the dimensions we can compare by
            # the crash values
            np.array([
                min(25, (ping["payload/info/subsessionLength"] or 0) / 3600.0), # usage hours, limited to ~25 hours to keep things normalized
                int(ping["meta/docType"] == "crash"), # main crash (is a crash ping)
                ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/content_parent"] or 0, # content process crashes
                (ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/plugin_parent"] or 0), # plugin crashes
                (ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/gmplugin_parent"] or 0), # GMplugin crashes
            ])
        )
    crash_values = ping_properties.filter(is_valid).map(get_crash_pair).reduceByKey(lambda a, b: a + b)

    def dimension_mapping(pair): # responsible for converting aggregate crash pairs into individual dimension fields
        dimension_key = pair[0]
        (submission_date, activity_date), dimension_values = dimension_key[:2], dimension_key[2:]
        usage_hours, main_crashes, content_crashes, plugin_crashes, gmplugin_crashes = pair[1]
        return (
            submission_date, activity_date,
            {
                key: str(dimension_value) if dimension_value is not None else ""
                for key, dimension_value in zip(dimension_names, dimension_values)
            },
            {
                "usage_hours": float(usage_hours),
                "main_crashes": float(main_crashes),
                "content_crashes": float(content_crashes),
                "plugin_crashes": float(plugin_crashes),
                "gmplugin_crashes": float(gmplugin_crashes),
            },
        )
    return crash_values.map(dimension_mapping)

def retrieve_crash_data(sc, submission_date_range, comparable_dimensions, fraction = 0.1):
    # get the raw data
    normal_pings = get_pings(
        sc, doc_type="main",
        submission_date=submission_date_range,
        fraction=fraction
    )
    crash_pings = get_pings(
        sc, doc_type="crash",
        submission_date=submission_date_range,
        fraction=fraction
    )

    return normal_pings.union(crash_pings)

def run_job(spark_context, sql_context, submission_date_range):
    """Fill the specified database with crash aggregates for the specified submission date range, creating the schema if needed."""
    start_date = datetime.strptime(submission_date_range[0], "%Y%m%d").date()
    end_date = datetime.strptime(submission_date_range[1], "%Y%m%d").date()

    schema = types.StructType([
        types.StructField("activity_date",   types.DateType(),                                            nullable=False),
        types.StructField("dimensions",      types.MapType(types.StringType(), types.StringType(), True), nullable=False),
        types.StructField("stats",           types.MapType(types.StringType(), types.DoubleType(), True), nullable=False),
    ])

    current_date = start_date
    while current_date <= end_date:
        # useful statements for testing the program
        #import sys, os; sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test")); import dataset; pings = sc.parallelize(list(dataset.generate_pings())) # use test pings; very good for debugging queries

        pings = retrieve_crash_data(spark_context, current_date.strftime("%Y%m%d"), COMPARABLE_DIMENSIONS, FRACTION)
        result = compare_crashes(pings, current_date, current_date, COMPARABLE_DIMENSIONS, DIMENSION_NAMES).coalesce(1)
        df = sql_context.createDataFrame(result, schema)
        print("SUCCESSFULLY COMPUTED CRASH AGGREGATES FOR {}".format(current_date))

        # upload the dataframe as Parquet to S3
        s3_result_url = "s3n://telemetry-test-bucket/crash_aggregates/v1/submission_date={}".format(current_date)
        df.saveAsParquetFile(s3_result_url)

        print("SUCCESSFULLY UPLOADED CRASH AGGREGATES FOR {} TO S3".format(current_date))

        current_date += timedelta(days=1)

    print("========================================")
    print("JOB COMPLETED SUCCESSFULLY")
    print("========================================")

if __name__ == "__main__":
    import argparse

    import sys, os
    try:
        sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
    except KeyError:
        print "SPARK_HOME not set"
        sys.exit(1)
    from pyspark import SparkContext
    from pyspark.sql import SQLContext

    yesterday_utc = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description="Fill a Postgresql database with crash rate aggregates for a certain date range.")
    parser.add_argument("--min-submission-date", help="Earliest date to include in the aggregate calculation in YYYYMMDD format (defaults to the current UTC date)", default=yesterday_utc)
    parser.add_argument("--max-submission-date", help="Latest date to include in the aggregate calculation in YYYYMMDD format (defaults to yesterday's UTC date)", default=yesterday_utc)
    args = parser.parse_args()
    submission_date_range = (args.min_submission_date, args.max_submission_date)

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    #sc = SparkContext(master="local[1]") # run sequentially with only 1 worker (useful for debugging)
    run_job(sc, sqlContext, submission_date_range)
