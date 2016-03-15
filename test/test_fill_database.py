#!/usr/bin/env bash

import unittest
import logging
import re
from datetime import datetime

import sys, os
try:
    sys.path.append(os.path.join(os.environ['SPARK_HOME'], "python"))
except KeyError:
    print "SPARK_HOME not set"
    sys.exit(1)
import pyspark

import dataset

import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from crash_rate_aggregates import fill_database

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.sc = pyspark.SparkContext(master="local[1]")
        self.raw_pings = self.sc.parallelize(list(dataset.generate_pings()))
        self.crash_rate_aggregates = fill_database.compare_crashes(
            self.raw_pings,
            fill_database.COMPARABLE_DIMENSIONS,
            fill_database.DIMENSION_NAMES
        ).collect()

    def tearDown(self):
        self.sc.stop()

    def test_length(self):
        self.assertEqual(self.raw_pings.count(), 8192)
        self.assertEqual(len(self.crash_rate_aggregates), 4096)

    def test_submission_date(self):
        for submission_date, dimensions, crashes in self.crash_rate_aggregates:
            self.assertIn(submission_date, {datetime(2016, 3, 5), datetime(2016, 6, 7)})

    def test_keys(self):
        for submission_date, dimensions, crashes in self.crash_rate_aggregates:
            self.assertTrue(re.match("^\d+(?:\.\d+(?:[a-z]\d+)?)?$", dimensions["build_version"]), dimensions["build_version"])
            self.assertTrue(re.match("^\d{14}$", dimensions["build_id"]), dimensions["build_id"])
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
        for submission_date, dimensions, crashes in self.crash_rate_aggregates:
            usage_hours, main_crashes, content_crashes, plugin_crashes = crashes
            self.assertEqual(usage_hours, 42 * 2 / 3600.0)
            self.assertEqual(main_crashes, 1)
            self.assertEqual(content_crashes, 42 * 2)
            self.assertEqual(plugin_crashes, 42 * 4)

if __name__ == '__main__':
    unittest.main()