#!/usr/bin/env python

import re
import sys
import os
import sys
import numpy as np
import dateutil.parser
import pyspark.sql.types as types

from moztelemetry.spark import get_pings, get_pings_properties
from datetime import datetime, date, timedelta

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
    "build_id",
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
    ], with_processes=True) # we only want the parent process data - child processes can also have plugin children, but we want to exclude those
    def is_valid(ping): # sanity check to make sure the ping is actually usable for our purposes
        submission_date = ping["meta/submissionDate"]
        if not isinstance(submission_date, (str, unicode)):
            return False
        activity_date = ping["creationDate"]
        if not isinstance(activity_date, (str, unicode)):
            return False
        subsession_length = ping["payload/info/subsessionLength"]
        if subsession_length is not None and not isinstance(subsession_length, (int, long)):
            return False
        build_id = ping["environment/build/buildId"]
        if not isinstance(build_id, (str, unicode)):
            return False
        if not re.match(r"^\d{14}$", build_id): # only accept valid buildids
            return False
        return True

    def get_crash_pair(ping): # responsible for normalizing a single ping into a crash pair
        # we need to parse and normalize the dates here rather than at the aggregates level,
        # because we need to normalize and get rid of the time portion

        # date the ping was received
        submission_date = datetime.strptime(ping["meta/submissionDate"], "%Y%m%d").date() # convert the YYYYMMDD format to a real date

        # date the ping was created on the client
        try:
            activity_date = dateutil.parser.parse(ping["creationDate"]).date() # the activity date is the date portion of creationDate
        except: # malformed date, like how sometimes seconds that are not in [0, 59]
            activity_date = submission_date # we'll just set it to the submission date, since we can't parse the activity date
        activity_date = max(submission_date - timedelta(days=7), min(submission_date, activity_date)) # normalize the activity date if it's out of range

        usage_hours = min(25, (ping["payload/info/subsessionLength"] or 0) / 3600.0) # usage hours, limited to ~25 hours to keep things normalized
        main_crashes = int(ping["meta/docType"] == "crash") # main process crash (is a crash ping)
        content_crashes = ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/content_parent"] or 0
        plugin_crashes = ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/plugin_parent"] or 0
        gecko_media_plugin_crashes = ping["payload/keyedHistograms/SUBPROCESS_CRASHES_WITH_DUMP/gmplugin_parent"] or 0
        return (
            (submission_date, activity_date) + tuple(ping[key] for key in comparable_dimensions), # all the dimensions we can compare by
            [ # the crash values
                1, # number of pings represented by the aggregate
                usage_hours, main_crashes, content_crashes, plugin_crashes, gecko_media_plugin_crashes,

                # squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
                usage_hours ** 2, main_crashes ** 2, content_crashes ** 2, plugin_crashes ** 2, gecko_media_plugin_crashes ** 2,
            ]
        )
    crash_values = ping_properties.filter(is_valid).map(get_crash_pair).reduceByKey(lambda a, b: [x + y for x, y in zip(a, b)])

    def dimension_mapping(pair): # responsible for converting aggregate crash pairs into individual dimension fields
        dimension_key, stats_array = pair
        (submission_date, activity_date), dimension_values = dimension_key[:2], dimension_key[2:]
        keys = [
            "ping_count",
            "usage_hours", "main_crashes", "content_crashes", "plugin_crashes", "gmplugin_crashes",
            "usage_hours_squared", "main_crashes_squared", "content_crashes_squared", "plugin_crashes_squared", "gmplugin_crashes_squared",
        ]
        return (
            submission_date, activity_date,
            {
                key: str(dimension_value).encode("ascii", errors="replace")
                for key, dimension_value in zip(dimension_names, dimension_values)
                if dimension_value is not None # only include the keys for which the value isn't null
            },
            dict(zip(keys, stats_array)), # dictionary mapping keys to their corresponding stats
        )
    return crash_values.map(dimension_mapping)

def retrieve_crash_data(sc, submission_date_range, comparable_dimensions, fraction):
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

def run_job(spark_context, sql_context, submission_date_range, use_test_data = False):
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
        if use_test_data:
            # use test pings; very good for debugging the uploading process
            sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test")) # make the dataset library importable
            import dataset
            pings = sc.parallelize(list(dataset.generate_pings()))
        else:
            pings = retrieve_crash_data(spark_context, current_date.strftime("%Y%m%d"), COMPARABLE_DIMENSIONS, FRACTION)

        result = compare_crashes(pings, current_date, current_date, COMPARABLE_DIMENSIONS, DIMENSION_NAMES).coalesce(1)
        df = sql_context.createDataFrame(result, schema)
        print("SUCCESSFULLY COMPUTED CRASH AGGREGATES FOR {}".format(current_date))

        # upload the dataframe as Parquet to S3
        s3_result_url = "s3n://telemetry-test-bucket/crash_aggregates/v1/submission_date={}".format(current_date)
        df.write.parquet(s3_result_url)

        print("SUCCESSFULLY UPLOADED CRASH AGGREGATES FOR {} TO S3".format(current_date))

        current_date += timedelta(days=1)

    print("========================================")
    print("JOB COMPLETED SUCCESSFULLY")
    print("========================================")

if __name__ == "__main__":
    import argparse
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
    parser.add_argument("--use-test-data", help="Generate and process test data instead of using real aggregates (this is much faster when doing testing)", type=bool, default=False)
    args = parser.parse_args()
    submission_date_range = (args.min_submission_date, args.max_submission_date)
    use_test_data = args.use_test_data

    sc = SparkContext()
    sqlContext = SQLContext(sc)
    #sc = SparkContext(master="local[1]") # run sequentially with only 1 worker (useful for debugging)
    run_job(sc, sqlContext, submission_date_range, use_test_data=use_test_data)
