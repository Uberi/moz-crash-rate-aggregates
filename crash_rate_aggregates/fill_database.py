#!/usr/bin/env python

import re
import sys
import os
import sys
import numpy as np
import dateutil.parser
import pyspark.sql.types as types

from moztelemetry.spark import get_pings, Histogram
from datetime import datetime, date, timedelta

FRACTION = 1.0

# paths/dimensions within the ping to compare by
COMPARABLE_DIMENSIONS = [
    ("environment", "build", "version"),
    ("environment", "build", "buildId"),
    ("application", "channel"),
    ("application", "name"),
    ("environment", "system", "os", "name"),
    ("environment", "system", "os", "version"),
    ("environment", "build", "architecture"),
    ("meta", "geoCountry"),
    ("environment", "addons", "activeExperiment", "id"),
    ("environment", "addons", "activeExperiment", "branch"),
    ("environment", "settings", "e10sEnabled"),
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

assert len(COMPARABLE_DIMENSIONS) == len(DIMENSION_NAMES), "COMPARABLE_DIMENSIONS and DIMENSION_NAMES must match"

def compare_crashes(pings, start_date, end_date, comparable_dimensions, dimension_names):
    """Returns a PairRDD where keys are user configurations and values are Numpy arrays of the form [usage hours, main process crashes, content process crashes, plugin crashes]"""
    def get_property(value, path):
        for key in path:
            if not isinstance(value, dict) or key not in value:
                return None
            value = value[key]
        return value

    def get_properties(ping):
        result = {
            key: get_property(ping, path)
            for key, path in zip(dimension_names, comparable_dimensions)
        }

        submission_date = get_property(ping, ("meta", "submissionDate"))
        result["submission_date"] = datetime.strptime(submission_date, "%Y%m%d").date() # convert the YYYYMMDD format to a real date

        activity_date = get_property(ping, ("creationDate",))
        try:
            result["activity_date"] = dateutil.parser.parse(activity_date).date() # the activity date is the date portion of creationDate
        except: # malformed date, like how sometimes seconds that are not in [0, 59]
            result["activity_date"] = None

        result["subsession_length"] = get_property(ping, ("payload", "info", "subsessionLength"))
        result["doc_type"] = get_property(ping, ("meta", "docType"))

        subprocess_crash_content = get_property(ping, ("payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "content"))
        result["subprocess_crash_content"] = 0 if subprocess_crash_content is None else Histogram("SUBPROCESS_CRASHES_WITH_DUMP", subprocess_crash_content).get_value()

        subprocess_crash_plugin = get_property(ping, ("payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "plugin"))
        result["subprocess_crash_plugin"] = 0 if subprocess_crash_plugin is None else Histogram("SUBPROCESS_CRASHES_WITH_DUMP", subprocess_crash_plugin).get_value()

        subprocess_crash_gmplugin = get_property(ping, ("payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin"))
        result["subprocess_crash_gmplugin"] = 0 if subprocess_crash_gmplugin is None else Histogram("SUBPROCESS_CRASHES_WITH_DUMP", subprocess_crash_gmplugin).get_value()
        return result

    def is_valid(ping): # sanity check to make sure the ping is actually usable for our purposes
        if not isinstance(ping["submission_date"], date):
            return False
        if not isinstance(ping["activity_date"], date):
            return False
        subsession_length = ping["subsession_length"]
        if subsession_length is not None and not isinstance(subsession_length, (int, long)):
            return False
        if not isinstance(ping["build_id"], (str, unicode)):
            return False
        if not re.match(r"^\d{14}$", ping["build_id"]): # only accept valid buildids
            return False
        return True

    original_count = pings.count()
    ping_properties = pings.map(get_properties).filter(is_valid).cache()
    filtered_count = ping_properties.count()

    def get_crash_pair(ping): # responsible for normalizing a single ping into a crash pair
        submission_date = ping["submission_date"] # convert the YYYYMMDD format to a real date
        activity_date = max(submission_date - timedelta(days=7), min(submission_date, ping["activity_date"])) # normalize the activity date if it's out of range

        usage_hours = min(25, (ping["subsession_length"] or 0) / 3600.0) # usage hours, limited to ~25 hours to keep things normalized
        main_crashes = int(ping["doc_type"] == "crash") # main process crash (is a crash ping)
        content_crashes = ping["subprocess_crash_content"] or 0
        plugin_crashes = ping["subprocess_crash_plugin"] or 0
        gecko_media_plugin_crashes = ping["subprocess_crash_gmplugin"] or 0
        return (
            (submission_date, activity_date) + tuple(ping[key] for key in dimension_names), # all the dimensions we can compare by
            [ # the crash values
                1, # number of pings represented by the aggregate
                usage_hours, main_crashes, content_crashes, plugin_crashes, gecko_media_plugin_crashes,

                # squared versions in order to compute stddev (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
                usage_hours ** 2, main_crashes ** 2, content_crashes ** 2, plugin_crashes ** 2, gecko_media_plugin_crashes ** 2,
            ]
        )

    crash_values = ping_properties.map(get_crash_pair).reduceByKey(lambda a, b: [x + y for x, y in zip(a, b)])

    def dimension_mapping(pair): # responsible for converting aggregate crash pairs into individual dimension fields
        dimension_key, stats_array = pair
        (submission_date, activity_date), dimension_values = dimension_key[:2], dimension_key[2:]
        stats_names = [
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
            dict(zip(stats_names, stats_array)), # dictionary mapping keys to their corresponding stats
        )

    return crash_values.map(dimension_mapping), original_count, filtered_count

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

        result, original_count, filtered_count = compare_crashes(pings, current_date, current_date, COMPARABLE_DIMENSIONS, DIMENSION_NAMES)
        result = result.coalesce(1) # put everything into a single partition
        df = sql_context.createDataFrame(result, schema)
        print("SUCCESSFULLY COMPUTED CRASH AGGREGATES FOR {}".format(current_date))

        # upload the dataframe as Parquet to S3
        s3_result_url = "s3n://telemetry-test-bucket/crash_aggregates/v1/submission_date={}".format(current_date)
        df.write.parquet(s3_result_url)

        print("SUCCESSFULLY UPLOADED CRASH AGGREGATES FOR {} TO S3".format(current_date))

        current_date += timedelta(days=1)

    print("========================================")
    print("JOB COMPLETED SUCCESSFULLY")
    print("{} pings processed, {} pings ignored".format(filtered_count, original_count - filtered_count))
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
