#!/usr/bin/env python

import re
import sys
import os
import dateutil.parser
import pyspark.sql.types as types

from moztelemetry.spark import get_pings
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
    ("environment", "settings", "e10sCohort"),
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
    "e10s_cohort",
]

assert len(COMPARABLE_DIMENSIONS) == len(DIMENSION_NAMES), \
       "COMPARABLE_DIMENSIONS and DIMENSION_NAMES must match"


def compare_crashes(spark_context, pings, comparable_dimensions, dimension_names):
    """
    Returns a PairRDD of crash aggregates, as well as the number of pings
    ignored due to being invalid.
    """
    def get_property(value, path):
        for key in path:
            if not isinstance(value, dict) or key not in value:
                return None
            value = value[key]
        return value

    def get_count_histogram_value(histogram):
        if not isinstance(histogram, dict):
            return 0  # definitely not a histogram, maybe it isn't even present
        if "values" not in histogram:
            return 0  # malformed or invalid histogram
        return long(histogram["values"].get("0", 0))

    def get_properties(ping):
        result = {
            key: get_property(ping, path)
            for key, path in zip(dimension_names, comparable_dimensions)
        }

        # convert the YYYYMMDD format to a real date
        submission_date = get_property(ping, ("meta", "submissionDate"))
        try:
            result["submission_date"] = datetime.strptime(submission_date, "%Y%m%d").date()
        except: # malformed date, which is very rare but does happen
            result["submission_date"] = None

        activity_date = get_property(ping, ("creationDate",))
        try:
            # the activity date is the date portion of creationDate
            result["activity_date"] = dateutil.parser.parse(activity_date).date()
        except:  # malformed date, like how sometimes seconds that are not in [0, 59]
            result["activity_date"] = None

        # subsession lengths are None for crash pings, since we don't keep track of that for those
        result["subsession_length"] = get_property(ping, ("payload", "info", "subsessionLength"))

        # this should always be valid as get_pings filters this for us
        result["doc_type"] = get_property(ping, ("meta", "docType"))

        result["subprocess_crash_content"] = get_count_histogram_value(
            get_property(ping, (
                "payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "content"
            ))
        )
        result["subprocess_crash_plugin"] = get_count_histogram_value(
            get_property(ping, (
                "payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "plugin"
            ))
        )
        result["subprocess_crash_gmplugin"] = get_count_histogram_value(
            get_property(ping, (
                "payload", "keyedHistograms", "SUBPROCESS_CRASHES_WITH_DUMP", "gmplugin"
            ))
        )
        return result

    ignored_pings_accumulator = spark_context.accumulator(0)

    def is_valid(ping):  # sanity check to make sure the ping is actually usable for our purposes
        subsession_length = ping["subsession_length"]
        if (
            isinstance(ping["submission_date"], date) and
            isinstance(ping["activity_date"], date) and
            (subsession_length is None or isinstance(subsession_length, (int, long))) and
            isinstance(ping["build_id"], (str, unicode)) and
            re.match(r"^\d{14}$", ping["build_id"])  # only accept valid buildids
        ):
            return True
        ignored_pings_accumulator.add(1)
        return False

    ping_properties = pings.map(get_properties).filter(is_valid)

    def get_crash_pair(ping):  # responsible for normalizing a single ping into a crash pair
        submission_date = ping["submission_date"]  # convert the YYYYMMDD format to a real date

        # normalize the activity date if it's out of range
        activity_date = max(
            submission_date - timedelta(days=7),
            min(submission_date, ping["activity_date"])
        )

        # usage hours, limited to ~25 hours to keep things normalized
        usage_hours = min(25, (ping["subsession_length"] or 0) / 3600.0)

        main_crashes = int(ping["doc_type"] == "crash")  # main process crash (is a crash ping)
        content_crashes = ping["subprocess_crash_content"] or 0
        plugin_crashes = ping["subprocess_crash_plugin"] or 0
        gecko_media_plugin_crashes = ping["subprocess_crash_gmplugin"] or 0
        return (
            # all the dimensions we can compare by
            (activity_date,) + tuple(ping[key] for key in dimension_names),

            # the crash values
            [
                1,  # number of pings represented by the aggregate
                usage_hours, main_crashes, content_crashes,
                plugin_crashes, gecko_media_plugin_crashes,

                # squared versions in order to compute stddev
                # (with $$\sigma = \sqrt{\frac{\sum X^2}{N} - \mu^2}$$)
                usage_hours ** 2, main_crashes ** 2, content_crashes ** 2,
                plugin_crashes ** 2, gecko_media_plugin_crashes ** 2,
            ]
        )

    crash_values = (
        ping_properties.map(get_crash_pair)
                       .reduceByKey(lambda a, b: [x + y for x, y in zip(a, b)])
    )

    def dimension_mapping(pair):
        """Converts aggregate crash pairs into individual dimension fields."""
        dimension_key, stats_array = pair
        (activity_date,), dimension_values = dimension_key[:1], dimension_key[1:]
        stats_names = [
            "ping_count",
            "usage_hours", "main_crashes", "content_crashes",
            "plugin_crashes", "gmplugin_crashes",
            "usage_hours_squared", "main_crashes_squared", "content_crashes_squared",
            "plugin_crashes_squared", "gmplugin_crashes_squared",
        ]
        return (
            activity_date.strftime("%Y-%m-%d"),
            {
                key: str(dimension_value).decode("ascii", errors="replace")
                for key, dimension_value in zip(dimension_names, dimension_values)
                if dimension_value is not None  # only include the keys with non-null values
            },
            dict(zip(stats_names, map(float, stats_array))),  # map of keys to corresponding stats
        )

    return crash_values.map(dimension_mapping), ignored_pings_accumulator


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


def run_job(spark_context, sql_context, submission_date_range, use_test_data=False):
    """
    Compute crash aggregates for the specified submission date range,
    and upload the result to S3.
    """
    start_date = datetime.strptime(submission_date_range[0], "%Y%m%d").date()
    end_date = datetime.strptime(submission_date_range[1], "%Y%m%d").date()

    schema = types.StructType([
        types.StructField(
            "activity_date",
            types.StringType(),
            nullable=False
        ),
        types.StructField(
            "dimensions",
            types.MapType(types.StringType(), types.StringType(), True),
            nullable=False
        ),
        types.StructField(
            "stats",
            types.MapType(types.StringType(), types.DoubleType(), True),
            nullable=False
        ),
    ])

    current_date = start_date
    while current_date <= end_date:
        # useful statements for testing the program
        if use_test_data:
            # use test pings; very good for debugging the uploading process
            sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test"))
            import dataset
            pings = sc.parallelize(list(dataset.generate_pings()))
        else:
            pings = retrieve_crash_data(
                spark_context,
                current_date.strftime("%Y%m%d"),
                COMPARABLE_DIMENSIONS, FRACTION
            )

        result, ignored_count = compare_crashes(
            spark_context,
            pings,
            COMPARABLE_DIMENSIONS, DIMENSION_NAMES
        )
        result = result.coalesce(1)  # put everything into a single partition
        df = sql_context.createDataFrame(result, schema)
        print("SUCCESSFULLY COMPUTED CRASH AGGREGATES FOR {}".format(current_date))

        # upload the dataframe as Parquet to S3
        s3_result_url = (
            "s3n://telemetry-test-bucket/crash_aggregates/v1/submission_date={}".format(
                current_date
            )
        )
        df.write.parquet(s3_result_url)

        print("SUCCESSFULLY UPLOADED CRASH AGGREGATES FOR {} TO S3".format(current_date))

        current_date += timedelta(days=1)

    print("========================================")
    print("JOB COMPLETED SUCCESSFULLY")
    print("{} pings ignored".format(ignored_count.value))
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
    parser = argparse.ArgumentParser(
        description="Aggregate crash statistics and upload them to S3 for a given date range."
    )
    parser.add_argument(
        "--min-submission-date",
        help="Earliest date to include in the aggregate calculation in YYYYMMDD format "
             "(defaults to the current UTC date)",
        default=yesterday_utc
    )
    parser.add_argument(
        "--max-submission-date",
        help="Latest date to include in the aggregate calculation in YYYYMMDD format "
             "(defaults to yesterday's UTC date)",
        default=yesterday_utc
    )
    parser.add_argument(
        "--use-test-data",
        help="Generate and process test data instead of using real aggregates "
             "(this is much faster when doing testing)",
        type=bool,
        default=False
    )
    args = parser.parse_args()
    submission_date_range = (args.min_submission_date, args.max_submission_date)
    use_test_data = args.use_test_data

    if args.use_test_data:
        # run sequentially with only 1 worker (useful for debugging)
        sc = SparkContext(master="local[1]")
    else:
        sc = SparkContext()
    sqlContext = SQLContext(sc)
    run_job(sc, sqlContext, submission_date_range, use_test_data=use_test_data)
