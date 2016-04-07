#!/usr/bin/env bash

import unittest
import logging
import re
from datetime import date

import sys, os
try:
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
except KeyError:
    print "SPARK_HOME not set"
    sys.exit(1)
import pyspark

import dataset

import sys, os
from crash_rate_aggregates.fill_database import compare_crashes, COMPARABLE_DIMENSIONS, DIMENSION_NAMES

# these dimensions are used to compute aggregate keys, but they're stored as separate columns
COLUMN_DIMENSIONS = ["submission_date", "activity_date"]

# these dimensions are folded and not used to calculate aggregate keys
# in other words, the value of these dimensions doesn't affect which aggregate they are placed in
# though they can affect the stats within each aggregate
FOLDED_DIMENSIONS = ["doc_type"]

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.sc = pyspark.SparkContext(master="local[1]")
        self.raw_pings = self.sc.parallelize(list(dataset.generate_pings()))

        result, self.original_count, self.filtered_count = compare_crashes(
            self.raw_pings,
            date(2016, 3, 5), date(2016, 6, 7),
            COMPARABLE_DIMENSIONS, DIMENSION_NAMES
        )
        self.crash_rate_aggregates = result.collect()

    def tearDown(self):
        self.sc.stop()

    def test_length(self):
        expected_pings = 2 ** (
            len(COMPARABLE_DIMENSIONS) +
            len(COLUMN_DIMENSIONS) +
            len(FOLDED_DIMENSIONS)
        )
        self.assertEqual(self.raw_pings.count(), expected_pings)
        self.assertEqual(self.original_count, self.raw_pings.count())
        self.assertEqual(self.original_count, self.filtered_count)
        self.assertEqual(len(self.crash_rate_aggregates), expected_pings / 2) # the doc_type dimension should be collapsed by compare_crashes

    def test_submission_date(self):
        for submission_date, activity_date, dimensions, crashes in self.crash_rate_aggregates:
            self.assertIn(submission_date, {date(2016, 3, 5), date(2016, 6, 7)})

    def test_activity_date(self):
        for submission_date, activity_date, dimensions, crashes in self.crash_rate_aggregates:
            self.assertIn(activity_date, {
                date(2016, 3, 2), date(2016, 6, 1), # these are directly from the dataset
                date(2016, 3, 5), date(2016, 5, 31), # these are normalized to be around the time of the submission date
            })

    def test_keys(self):
        for submission_date, activity_date, dimensions, stats in self.crash_rate_aggregates:
            self.assertIn(dimensions["build_version"],     dataset.ping_dimensions["build_version"])
            self.assertIn(dimensions["build_id"],          dataset.ping_dimensions["build_id"])
            self.assertIn(dimensions["channel"],           dataset.ping_dimensions["channel"])
            self.assertIn(dimensions["application"],       dataset.ping_dimensions["application"])
            self.assertIn(dimensions["os_name"],           dataset.ping_dimensions["os_name"])
            self.assertIn(dimensions["os_version"],        dataset.ping_dimensions["os_version"])
            self.assertIn(dimensions["architecture"],      dataset.ping_dimensions["architecture"])
            self.assertIn(dimensions["country"],           dataset.ping_dimensions["country"])
            self.assertIn(dimensions.get("experiment_id"), dataset.ping_dimensions["experiment_id"])
            self.assertIn(dimensions["experiment_branch"], dataset.ping_dimensions["experiment_branch"])
            self.assertIn(dimensions["e10s_enabled"],      ["True", "False"])

    def test_crash_rates(self):
        for submission_date, activity_date, dimensions, stats in self.crash_rate_aggregates:
            self.assertEqual(stats["ping_count"], 2)
            self.assertEqual(stats["usage_hours"], 42 * 2 / 3600.0)
            self.assertEqual(stats["main_crashes"], 1)
            self.assertEqual(stats["content_crashes"], 42 * 2)
            self.assertEqual(stats["plugin_crashes"], 42 * 2)
            self.assertEqual(stats["gmplugin_crashes"], 42 * 2)
            self.assertEqual(stats["usage_hours_squared"], 0.00027222222222222226)
            self.assertEqual(stats["main_crashes_squared"], 1)
            self.assertEqual(stats["content_crashes_squared"], 3528)
            self.assertEqual(stats["plugin_crashes_squared"], 3528)
            self.assertEqual(stats["gmplugin_crashes_squared"], 3528)

if __name__ == '__main__':
    unittest.main()