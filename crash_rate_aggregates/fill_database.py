from datetime import datetime

import psycopg2
import numpy as np

from moztelemetry.spark import get_pings, get_pings_properties
from pyspark import SparkContext

SUBMISSION_DATE_RANGE = (datetime.utcnow().strftime("%Y%m%d"),) * 2
FRACTION = 0.1

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

def compare_crashes(pings, comparable_dimensions):
    """Returns a PairRDD where keys are user configurations and values are Numpy arrays of the form [usage hours, main process crashes, content process crashes, plugin crashes]"""
    ping_properties = get_pings_properties(pings, comparable_dimensions + [
        "payload/info/subsessionLength",
        "meta/submissionDate",
        "meta/reason",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin",
        "payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin",
    ])
    return ping_properties.map(lambda p: (
        # the keys we want to filter based on
        (p["meta/submissionDate"],) + tuple(p[key] for key in comparable_dimensions),
        np.array([
            (p["payload/info/subsessionLength"] or 0) / 3600.0,
            int(p["meta/reason"] == "aborted-session"), # main process crashes
            p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/content"] or 0, # content process crashes
            (p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/plugin"] or 0) +
            (p["payload/keyedHistograms/SUBPROCESS_ABNORMAL_ABORT/gmplugin"] or 0) # plugin crashes
        ])
    )).reduceByKey(lambda a, b: a + b)

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

if __name__ == "__main__":
    sc = SparkContext()
    pings = retrieve_crash_data(sc, SUBMISSION_DATE_RANGE, COMPARABLE_DIMENSIONS, FRACTION)

    conn = psycopg2.connect(database="aggregates", user="postgres")
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS aggregates (
        id serial PRIMARY KEY,
        submission_date date,
        build_version varchar,
        build_id varchar,
        channel varchar,
        application varchar,
        os_name varchar,
        os_version varchar,
        architecture varchar,
        country varchar,
        experiment_id varchar,
        experiment_branch varchar,
        e10s_enabled varchar,
        usage_hours real,
        main_crashes real,
        content_crashes real,
        plugin_crashes real
    );
    """)

    # remove previous data for the selected days, if available
    # this is necessary to be able to backfill data properly
    cur.execute(
        """DELETE FROM aggregates WHERE submission_date >= %s and submission_date <= %s""".format(", ".join(DIMENSION_NAMES)),
        (datetime.strptime(SUBMISSION_DATE_RANGE[0], "%Y%m%d").date(), datetime.strptime(SUBMISSION_DATE_RANGE[1], "%Y%m%d").date())
    )

    result = compare_crashes(pings, COMPARABLE_DIMENSIONS)
    for dimension_values, crash_data in result.toLocalIterator():
        submission_date, dimension_values = dimension_values[0], dimension_values[1:]
        submission_date = datetime.strptime(submission_date, "%Y%m%d")
        usage_hours, main_crashes, content_crashes, plugin_crashes = crash_data
        cur.execute(
            """INSERT INTO aggregates(submission_date, {}, usage_hours, main_crashes, content_crashes, plugin_crashes) VALUES (%s, {}%s, %s, %s, %s)""".format(
                ", ".join(DIMENSION_NAMES), "%s, " * len(DIMENSION_NAMES)
            ),
            (submission_date,) + dimension_values + (usage_hours, main_crashes, content_crashes, plugin_crashes)
        )

    conn.commit()
    cur.close()
    conn.close()
