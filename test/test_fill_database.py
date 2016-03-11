#!/usr/bin/env bash

import unittest
import logging
import re

import pyspark

import dataset

import sys, os
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from crash_rate_aggregates import fill_database

class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.sc = pyspark.SparkContext(master="local[1]")
        self.raw_pings = self.sc.parallelize(list(dataset.generate_pings()))
        self.crash_rate_aggregates = fill_database.compare_crashes(self.raw_pings, fill_database.COMPARABLE_DIMENSIONS).collect()

    def tearDown(self):
        self.sc.stop()

    def test_length(self):
        self.assertEqual(self.raw_pings.count(), 4096)
        self.assertEqual(len(self.crash_rate_aggregates), 1024)

    def test_keys(self):
        for keys, crashes in self.crash_rate_aggregates:
            self.assertTrue(re.match("^\d{8}$", keys[0]), keys[0]) # submission date
            self.assertTrue(re.match("^\d+(?:\.\d+(?:[a-z]\d+)?)?$", keys[1]), keys[1]) # version
            self.assertTrue(re.match("^\d{14}$", keys[2]), keys[2]) # build ID
            self.assertIn(keys[3], {"nightly", "aurora", "beta", "release"}) # channel
            self.assertIn(keys[4], {"Linux", "Windows_NT", "Darwin"}) # OS name
            self.assertTrue(re.match("^[\d\.]+$", keys[5]), keys[5]) # OS version
            self.assertTrue(re.match("^[\w-]+$", keys[6]), keys[6]) # locale
            self.assertTrue(keys[7] is None or "@" in keys[7], keys[7]) # experiment ID
            self.assertTrue(re.match("^[\w-]+$", keys[8]), keys[8]) # experiment branch
            self.assertTrue(isinstance(keys[9], bool), keys[9]) # E10S enabled

    def test_crash_rates(self):
        for keys, crashes in self.crash_rate_aggregates:
            usage_hours, main_crashes, content_crashes, plugin_crashes = crashes
            self.assertEqual(usage_hours, 42 * 4 / 3600.0)
            self.assertEqual(main_crashes, 2)
            self.assertEqual(content_crashes, 42 * 4)
            self.assertEqual(plugin_crashes, 42 * 8)

if __name__ == '__main__':
    unittest.main()