#!/usr/bin/env python

import sys
from datetime import datetime, date

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

def compare_crashes(pings, comparable_dimensions, dimension_names):
    """Returns a PairRDD where keys are user configurations and values are Numpy arrays of the form [usage hours, main process crashes, content process crashes, plugin crashes]"""
    ping_properties = get_pings_properties(pings, comparable_dimensions + [
        "payload/info/subsessionLength",
        "meta/submissionDate",
        "meta/reason",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin",
    ])
    crash_values = ping_properties.map(lambda p: (
        # the keys we want to filter based on
        (p["meta/submissionDate"],) + tuple(p[key] for key in comparable_dimensions),
        # the crash values
        np.array([
            max(0, min(25, (p["payload/info/subsessionLength"] or 0) / 3600.0)),
            int(p["meta/reason"] == "aborted-session"), # main process crashes
            p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content"] or 0, # content process crashes
            (p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin"] or 0) +
            (p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin"] or 0) # plugin crashes
        ])
    )).reduceByKey(lambda a, b: a + b)

    def dimension_mapping(pair):
        dimension_key, aggregates = pair
        submission_date, dimension_values = dimension_key[0], dimension_key[1:]
        submission_date = datetime.strptime(submission_date, "%Y%m%d") # convert the YYYYMMDD format to a real datetime
        return (
            submission_date,
            {
                key: dimension_value
                for key, dimension_value in zip(dimension_names, dimension_values)
            },
            aggregates, # crash values
        )
    return crash_values.map(dimension_mapping)

def retrieve_crash_data(sc, submission_date_range, comparable_dimensions, fraction = 0.1):
    # get the raw data
    normal_pings = get_pings(
        sc,
        submission_date=submission_date_range,
        fraction=fraction
    )
    crash_pings = get_pings(
        sc, doc_type="main",
        submission_date=submission_date_range,
        fraction=fraction
    ).filter(lambda p: p.get("meta", {}).get("reason") == "aborted-session")

    return normal_pings.union(crash_pings)

def run_job(spark_context, submission_date_range, db_host, db_name, db_user, db_pass):
    start_date = datetime.strptime(submission_date_range[0], "%Y%m%d").date()
    end_date = datetime.strptime(submission_date_range[1], "%Y%m%d").date()

    pings = retrieve_crash_data(spark_context, submission_date_range, COMPARABLE_DIMENSIONS, FRACTION)

    # useful statements for testing the program
    #sc = SparkContext(master="local[1]") # run sequentially with only 1 worker
    #import sys, os; sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test")); import dataset; pings = sc.parallelize(list(dataset.generate_pings())) # use test pings; very good for testing queries

    # compare crashes by all of the above dimensions
    result = compare_crashes(pings, COMPARABLE_DIMENSIONS, DIMENSION_NAMES)

    conn = psycopg2.connect(host=db_host, database=db_name, user=db_user, password=db_pass)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS crash_aggregates (
        id serial PRIMARY KEY,
        submission_date date NOT NULL,
        dimensions jsonb,
        usage_hours real NOT NULL,
        main_crashes real NOT NULL,
        content_crashes real NOT NULL,
        plugin_crashes real NOT NULL
    );
    """)

    # create child tables that inherit from the crash_aggregates table; these partition the data by month for faster querying
    # when running queries, we can still select from the parent crash_aggregates table; the query will use the child tables as needed
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
            ) INHERITS (crash_aggregates);
            CREATE INDEX {table_name}_date_idx ON {table_name} (submission_date);
            CREATE INDEX {table_name}_dimension_idx ON {table_name} USING gin (dimensions);
        END IF;
        END
        $$
        """.format(
            table_name="crash_aggregates_partition_{}_{}".format(current_month.year, current_month.month),
            current_month=datetime.strftime(current_month, "%Y-%m-%d"),
            next_month=datetime.strftime(next_month, "%Y-%m-%d"),
        ))
        current_month = next_month

    # when we attempt to insert into crash_aggregates, redirect it to the proper child table instead
    # the child table will only store a month's worth of data (~3000000 rows), so queries that only need
    # a specific submission date range will have much less to look through
    cur.execute("""
    CREATE OR REPLACE FUNCTION crash_aggregates_insert_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
        EXECUTE format('INSERT INTO %I SELECT $1.*', 'crash_aggregates_partition_' || EXTRACT(YEAR FROM NEW.submission_date) || '_' || EXTRACT(MONTH FROM NEW.submission_date)) USING NEW;
        RETURN NEW;
    END;
    $$
    LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS insert_crash_aggregates_trigger ON crash_aggregates;
    CREATE TRIGGER insert_crash_aggregates_trigger BEFORE INSERT ON crash_aggregates FOR EACH ROW EXECUTE PROCEDURE crash_aggregates_insert_trigger();
    """)

    # remove previous data for the selected days, if available
    # this is necessary to be able to backfill data properly
    cur.execute("""DELETE FROM crash_aggregates WHERE submission_date >= %s and submission_date <= %s""", (start_date, end_date))

    for submission_date, dimension_values, crash_data in result.toLocalIterator():
        usage_hours, main_crashes, content_crashes, plugin_crashes = crash_data
        cur.execute(
            """INSERT INTO crash_aggregates(submission_date, dimensions, usage_hours, main_crashes, content_crashes, plugin_crashes) VALUES (%s, %s, %s, %s, %s, %s)""",
            (
                submission_date,
                extras.Json(dimension_values),
                usage_hours, main_crashes, content_crashes, plugin_crashes
            )
        )

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    import argparse

    import sys, os
    try:
        sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
    except KeyError:
        print "SPARK_HOME not set"
        sys.exit(1)
    from pyspark import SparkContext

    today_utc = datetime.utcnow().strftime("%Y%m%d")
    parser = argparse.ArgumentParser(description="Fill a Postgresql database with crash rate aggregates for a certain date range.")
    parser.add_argument("--min-submission-date", help="Earliest date to include in the aggregate calculation in YYYYMMDD format (defaults to the current UTC date)", default=today_utc)
    parser.add_argument("--max-submission-date", help="Latest date to include in the aggregate calculation in YYYYMMDD format (defaults to the current UTC date)", default=today_utc)
    parser.add_argument("--pg-host", help="Host/address of the Postgresql database", required=True)
    parser.add_argument("--pg-name", help="Name of the Postgresql database", required=True)
    parser.add_argument("--pg-username", help="Username for the Postgresql database", required=True)
    parser.add_argument("--pg-password", help="Password for the Postgresql database", required=True)
    args = parser.parse_args()
    args.min_submission_date, args.max_submission_date = "20160301", "20160630"
    submission_date_range = (args.min_submission_date, args.max_submission_date)
    db_host, db_name, db_user, db_pass = args.pg_host, args.pg_name, args.pg_username, args.pg_password

    sc = SparkContext()
    run_job(sc, submission_date_range, db_host, db_name, db_user, db_pass)
