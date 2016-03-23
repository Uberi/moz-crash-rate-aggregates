#!/usr/bin/env python

import re
import sys
from datetime import datetime, date, timedelta
import dateutil.parser

import psycopg2
from psycopg2 import extras
import numpy as np

from moztelemetry.spark import get_pings, get_pings_properties

FRACTION = 0.1

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
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin",
    ], with_processes=True)
    def is_valid(ping): # sanity check to make sure the ping is actually usable for our purposes
        return (
            (isinstance(ping["meta/submissionDate"], str) or isinstance(ping["meta/submissionDate"], unicode)) and
            (isinstance(ping["creationDate"], str) or isinstance(ping["creationDate"], unicode))
        )
    def get_crash_pair(ping): # responsible for normalizing a single ping into a crash pair
        # we need to parse and normalize the dates here rather than at the aggregates level,
        # because we need to normalize and get rid of the time portion

        # date the ping was received
        submission_date = datetime.strptime(ping["meta/submissionDate"], "%Y%m%d").date() # convert the YYYYMMDD format to a real date
        submission_date = max(start_date, min(end_date, submission_date)) # normalize the submission date if it's out of range

        # date the ping was created on the client
        activity_date = dateutil.parser.parse(ping["creationDate"]).date() # the activity date is the date portion of creationDate
        activity_date = max(submission_date - timedelta(days=7), min(submission_date, activity_date)) # normalize the activity date if it's out of range

        # get rid of the time portion of the timestamp, since it'll blow up the number of aggregates
        if isinstance(ping["environment/build/buildId"], str):
            ping["environment/build/buildId"] = ping["environment/build/buildId"][:8]

        return (
            # the keys we want to filter based on
            (submission_date, activity_date) + tuple(ping[key] for key in comparable_dimensions), # all the dimensions we can compare by
            # the crash values
            np.array([
                max(0, min(25, (ping["payload/info/subsessionLength"] or 0) / 3600.0)), # usage hours
                int(ping["meta/docType"] == "crash"), # main crash (is a crash ping)
                ping["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content_parent"] or 0, # content process crashes
                (ping["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin_parent"] or 0) +
                (ping["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin_parent"] or 0) # plugin crashes
            ])
        )
    crash_values = ping_properties.filter(is_valid).map(get_crash_pair).reduceByKey(lambda a, b: a + b)

    def dimension_mapping(pair): # responsible for converting aggregate crash pairs into individual dimension fields
        dimension_key = pair[0]
        (submission_date, activity_date), dimension_values = dimension_key[:2], dimension_key[2:]
        usage_hours, main_crashes, content_crashes, plugin_crashes = pair[1]
        return (
            submission_date, activity_date,
            {
                key: dimension_value
                for key, dimension_value in zip(dimension_names, dimension_values)
            },
            {
                "usage_hours": usage_hours,
                "main_crashes": main_crashes,
                "content_crashes": content_crashes,
                "plugin_crashes": plugin_crashes,
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

def run_job(spark_context, submission_date_range, db_host, db_name, db_user, db_pass):
    """Fill the specified database with crash aggregates for the specified submission date range, creating the schema if needed."""
    start_date = datetime.strptime(submission_date_range[0], "%Y%m%d").date()
    end_date = datetime.strptime(submission_date_range[1], "%Y%m%d").date()

    print("Retrieving pings for {}...".format(submission_date_range))
    pings = retrieve_crash_data(spark_context, submission_date_range, COMPARABLE_DIMENSIONS, FRACTION)

    # useful statements for testing the program
    #import sys, os; sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test")); import dataset; pings = sc.parallelize(list(dataset.generate_pings())) # use test pings; very good for debugging queries

    # compare crashes by all of the above dimensions
    print("Comparing crashes along dimensions {}...".format(DIMENSION_NAMES))
    result = compare_crashes(pings, start_date, end_date, COMPARABLE_DIMENSIONS, DIMENSION_NAMES)

    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass)
    cur = conn.cursor()

    print("Setting up database...")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS aggregates (
        id serial PRIMARY KEY,
        submission_date date NOT NULL,
        activity_date date NOT NULL,
        dimensions jsonb,
        stats jsonb
    );
    """)

    # create child tables that inherit from `aggregates`; these partition the data by month for faster querying
    # when running queries, we can still select from `aggregates`; the query will use the child tables as needed
    current_month = date(start_date.year, start_date.month, 1)
    while current_month <= end_date: # loop through the month range
        next_month = date(current_month.year, (current_month.month % 12) + 1, 1)
        cur.execute("""
        DO $$
        BEGIN
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}') THEN
            CREATE TABLE {table_name} (
                CONSTRAINT {table_name}_pk PRIMARY KEY (id),
                CONSTRAINT {table_name}_ck CHECK (submission_date >= DATE '{current_month}' AND submission_date < DATE '{next_month}')
            ) INHERITS (aggregates);
            CREATE INDEX {table_name}_date_idx ON {table_name} (submission_date);
            CREATE INDEX {table_name}_dimension_idx ON {table_name} USING gin (dimensions);
        END IF;
        END
        $$
        """.format(
            table_name="aggregates_partition_{}_{}".format(current_month.year, current_month.month),
            current_month=datetime.strftime(current_month, "%Y-%m-%d"),
            next_month=datetime.strftime(next_month, "%Y-%m-%d"),
        ))
        current_month = next_month

    # when we attempt to insert or delete into `aggregates`, redirect it to the proper child table instead
    # the child table will only store a month's worth of data (~3000000 rows), so queries that only need
    # a specific submission date range will have much less to look through
    cur.execute("""
    CREATE OR REPLACE FUNCTION aggregates_insert_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        EXECUTE format('INSERT INTO %I SELECT $1.*', 'aggregates_partition_' || EXTRACT(YEAR FROM NEW.submission_date) || '_' || EXTRACT(MONTH FROM NEW.submission_date)) USING NEW;
        RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS insert_aggregates_trigger ON aggregates;
    CREATE TRIGGER insert_aggregates_trigger BEFORE INSERT ON aggregates FOR EACH ROW EXECUTE PROCEDURE aggregates_insert_trigger();
    """)

    # remove previous data for the selected days, if available
    # this is necessary to be able to backfill data properly
    cur.execute("""DELETE FROM aggregates WHERE submission_date >= %s and submission_date <= %s""", (start_date, end_date))
    print("Removed {} existing aggregates for the submission date range {} to {}".format(cur.rowcount, start_date, end_date))

    print("Collecting and updating aggregates...")

    row_accumulator = [] # do inserts in large chunks for a significantly faster insertion operation while allowing for larger-than-RAM datasets
    aggregate_count = 0
    for submission_date, activity_date, dimensions, crash_data in result.toLocalIterator():
        aggregate_count += 1 # doing this is actually faster than using result.count()
        row_accumulator.append(cur.mogrify("(%s, %s, %s, %s)", (
            submission_date,
            activity_date,
            extras.Json(dimensions),
            extras.Json(crash_data),
        )))
        if len(row_accumulator) >= INSERT_CHUNK_SIZE: # full chunk obtained, perform insert
            cur.execute("""INSERT INTO aggregates(submission_date, activity_date, dimensions, stats) VALUES {}""".format(",".join(row_accumulator)))
            row_accumulator = []
        if aggregate_count % 10000 == 0:
            print("Collecting and updating aggregates... {} processed".format(aggregate_count))

    if row_accumulator: # handle any remaining rows that need to be inserted
        cur.execute("""INSERT INTO aggregates(submission_date, activity_date, dimensions, stats) VALUES {}""".format(",".join(row_accumulator)))

    conn.commit()
    cur.close()
    conn.close()

    print("========================================")
    print("JOB COMPLETED SUCCESSFULLY")
    print("inserted {} aggregates".format(aggregate_count))
    print("========================================")

def cleanup_old_tables(db_host, db_name, db_user, db_pass):
    """Drops partitions of the `aggregates` table that are older than 26 weeks."""
    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass)
    cur = conn.cursor()

    cur.execute("""SELECT relname FROM pg_class WHERE relkind = 'r' AND relname ~ '^(aggregates_partition_)';""")
    table_drop_count = 0
    for table_name, in cur:
        match = re.match("^aggregates_partition_(\d+)_(\d+)$", table_name)
        year, month = int(match.group(1)), int(match.group(2))
        weeks_since_partition_first_day = (date.today() - date(year, month, 1)).days / 7
        if weeks_since_partition_first_day > 26:
            print("Dropping table {} since it holds data older than 26 weeks.")
            table_drop_count += 1
            cur.execute("""DROP TABLE {};""".format(table_name))

    print("========================================")
    print("OLD TABLES CLEANED SUCCESSFULLY")
    print("dropped {} tables".format(table_drop_count))
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

    yesterday_utc = (datetime.utcnow() - timedelta(days=1)).strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description="Fill a Postgresql database with crash rate aggregates for a certain date range.")
    parser.add_argument("--min-submission-date", help="Earliest date to include in the aggregate calculation in YYYYMMDD format (defaults to the current UTC date)", default=yesterday_utc)
    parser.add_argument("--max-submission-date", help="Latest date to include in the aggregate calculation in YYYYMMDD format (defaults to yesterday's UTC date)", default=yesterday_utc)
    parser.add_argument("--pg-host", help="Host/address of the Postgresql database", required=True)
    parser.add_argument("--pg-name", help="Name of the Postgresql database", required=True)
    parser.add_argument("--pg-username", help="Username for the Postgresql database", required=True)
    parser.add_argument("--pg-password", help="Password for the Postgresql database", required=True)
    args = parser.parse_args()
    submission_date_range = (args.min_submission_date, args.max_submission_date)
    db_host, db_name, db_user, db_pass = args.pg_host, args.pg_name, args.pg_username, args.pg_password

    cleanup_old_tables(db_host, db_name, db_user, db_pass)

    sc = SparkContext()
    #sc = SparkContext(master="local[1]") # run sequentially with only 1 worker (useful for debugging)
    run_job(sc, submission_date_range, db_host, db_name, db_user, db_pass)
