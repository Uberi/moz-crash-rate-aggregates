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

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.sc = pyspark.SparkContext(master="local[1]")
        self.raw_pings = self.sc.parallelize(list(dataset.generate_pings()))

        self.crash_rate_aggregates = compare_crashes(
            self.raw_pings,
            date(2016, 3, 5), date(2016, 6, 7),
            COMPARABLE_DIMENSIONS, DIMENSION_NAMES
        ).collect()

    def tearDown(self):
        self.sc.stop()

    def test_length(self):
        expected_pings = 2 ** (len(COMPARABLE_DIMENSIONS) + 3) # the 2 extra dimensions are submission_date, activity_date, and doc_type
        self.assertEqual(self.raw_pings.count(), expected_pings)
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
            self.assertTrue(re.match("^\d+(?:\.\d+(?:[a-z]\d+)?)?$", dimensions["build_version"]), dimensions["build_version"])
            self.assertTrue(re.match("^\d{14}$", dimensions["build_date"]), dimensions["build_date"])
            self.assertIn(dimensions["channel"], {"nightly", "aurora", "beta", "release"})
            self.assertIn(dimensions["application"], {"Firefox", "Fennec"})
            self.assertIn(dimensions["os_name"], {"Linux", "Windows_NT", "Darwin"})
            self.assertTrue(re.match("^[\d\.]+$", dimensions["os_version"]), dimensions["os_version"])
            self.assertTrue(re.match("^[\w-]+$", dimensions["architecture"]), dimensions["architecture"])
            self.assertTrue(re.match("^[\w-]+$", dimensions["architecture"]), dimensions["country"])
            self.assertTrue(dimensions["experiment_id"] is None or "@" in dimensions["experiment_id"], dimensions["experiment_id"])
            self.assertTrue(re.match("^[\w-]+$", dimensions["experiment_branch"]), dimensions["experiment_branch"])
            self.assertTrue(isinstance(dimensions["e10s_enabled"], bool), dimensions["e10s_enabled"])

    def test_crash_rates(self):
        for submission_date, activity_date, dimensions, stats in self.crash_rate_aggregates:
            self.assertEqual(stats["usage_hours"], 42 * 2 / 3600.0)
            self.assertEqual(stats["main_crashes"], 1)
            self.assertEqual(stats["content_crashes"], 42 * 2)
            self.assertEqual(stats["plugin_crashes"], 42 * 4)

if __name__ == '__main__':
    unittest.main()