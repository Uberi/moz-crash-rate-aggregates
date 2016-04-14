Crash Rate Aggregates
=====================

[![Build Status](https://travis-ci.org/mozilla/moz-crash-rate-aggregates.svg?branch=master)](https://travis-ci.org/Uberi/moz-crash-rate-aggregates)

Scheduled batch job for computing aggregate crash rates across a variety of criteria.

* A Spark analysis runs daily using a [scheduled analysis job](https://analysis.telemetry.mozilla.org/cluster/schedule).
    * The job, `crash-aggregates`, runs the `run_crash_aggregator.ipynb` Jupyter notebook, which downloads, installs, and runs the crash-rate-aggregates on the cluster.
    * Currently, this job is running under :azhang's account every day at 1am UTC, with the default settings for everything else. The job is named `crash-aggregates`.
* The job uploads the resulting data to S3, under prefixes of the form `crash-aggregates/v1/submission_date=(YYYY-MM-DD SUBMISSION DATE)/` in the `telemetry-test-bucket` bucket.
    * Each of these prefixes is a partition. There is a partition for each submission date.
* We have [Presto](https://prestodb.io/) set up to query the data on S3 using Presto's SQL query engine.
    * Currently, this instance is available at `ec2-54-218-5-112.us-west-2.compute.amazonaws.com`. The [provisioning files for the Presto instance](https://github.com/vitillo/emr-bootstrap-presto) are available as well.
    * At the moment, new partitions must be imported using [parquet2hive](https://github.com/vitillo/parquet2hive). There's a temporary cron job set up on the instance to do the importing, which will [eventually be replaced with something better](https://bugzilla.mozilla.org/show_bug.cgi?id=1251648).
* The [re:dash](https://sql.telemetry.mozilla.org/dashboard/general) setup connects to Presto and allows users to make SQL queries, build dashboards, etc. with the crash aggregates data.

Schemas and Making Queries
--------------------------

In the re:dash interface, make a new query, and set the data source to "Presto" if it isn't already. Now, we can make queries against the `crash_aggregates` table, which contains all of our crash data.

A query that computes crash rates for each channel (sorted by number of usage hours) would look like this:

```sql
SELECT dimensions['channel'] AS channel,
       sum(stats['usage_hours']) AS usage_hours,
       1000 * sum(stats['main_crashes']) / sum(stats['usage_hours']) AS main_crash_rate,
       1000 * sum(stats['content_crashes']) / sum(stats['usage_hours']) AS content_crash_rate,
       1000 * sum(stats['plugin_crashes']) / sum(stats['usage_hours']) AS plugin_crash_rate,
       1000 * sum(stats['gmplugin_crashes']) / sum(stats['usage_hours']) AS gmplugin_crash_rate
FROM crash_aggregates
GROUP BY dimensions['channel']
ORDER BY -sum(stats['usage_hours'])
```

An aggregate in this context is a combined collection of Telemetry pings. An aggregate for Windows 7 64-bit Firefox on March 15, 2016 represents the stats for all pings that originate from Windows 7 64-bit Firefox, on March 15, 2016. Aggregates represent all the pings that meet that aggregate's criteria. The subpopulations represented by individual aggregates are always disjoint.

Presto has its own SQL engine, and therefore its own extensions to standard SQL. The SQL that Presto uses is documented [here](https://prestodb.io/docs/current/).

The `crash_aggregates` table has 4 commonly-used columns:

* `submission_date` is the date pings were submitted for a particular aggregate.
    * For example, `select sum(stats['usage_hours']) from crash_aggregates where submission_date = '2016-03-15'` will give the total number of user hours represented by pings submitted on March 15, 2016.
    * The dataset is partitioned by this field. Queries that limit the possible values of `submission_date` can run significantly faster.
* `activity_date` is the date pings were generated on the client for a particular aggregate.
    * For example, `select sum(stats['usage_hours']) from crash_aggregates where activity_date = '2016-03-15'` will give the total number of user hours represented by pings generated on March 15, 2016.
    * This can be several days before the pings are actually submitted, so it will always be before or on its corresponding `submission_date`.
    * Therefore, queries that are sensitive to when measurements were taken on the client should prefer this field over `submission_date`.
* `dimensions` is a map of all the other dimensions that we currently care about. These fields include:
    * `dimensions['build_version']` is the program version, like `46.0a1`.
    * `dimensions['build_id']` is the YYYYMMDDhhmmss timestamp the program was built, like `20160123180541`. This is also known as the "build ID" or "buildid".
    * `dimensions['channel']` is the channel, like `release` or `beta`.
    * `dimensions['application']` is the program name, like `Firefox` or `Fennec`.
    * `dimensions['os_name']` is the name of the OS the program is running on, like `Darwin` or `Windows_NT`.
    * `dimensions['os_version']` is the version of the OS the program is running on.
    * `dimensions['architecture']` is the architecture that the program was built for (not necessarily the one it is running on).
    * `dimensions['country']` is the country code for the user (determined using geoIP), like `US` or `UK`.
    * `dimensions['experiment_id']` is the identifier of the experiment being participated in, such as `e10s-beta46-noapz@experiments.mozilla.org`, or null if no experiment.
    * `dimensions['experiment_branch']` is the branch of the experiment being participated in, such as `control` or `experiment`, or null if no experiment.
    * `dimensions['e10s_enabled']` is whether E10S is enabled.
    * `dimensions['e10s_cohort']` is the E10S cohort the user is part of, such as `control`, `test`, or `disqualified`.
    * All of the above fields can potentially be blank, which means "not present". That means that in the actual pings, the corresponding fields were null.
* `stats` contains the aggregate values that we care about:
    * `stats['usage_hours']` is the number of user-hours represented by the aggregate.
    * `stats['main_crashes']` is the number of main process crashes represented by the aggregate (or just program crashes, in the non-E10S case).
    * `stats['content_crashes']` is the number of content process crashes represented by the aggregate.
    * `stats['plugin_crashes']` is the number of plugin process crashes represented by the aggregate.
    * `stats['gmplugin_crashes']` is the number of Gecko media plugin (often abbreviated GMPlugin) process crashes represented by the aggregate.

Plugin process crashes per hour on Nightly for March 14:

```sql
SELECT sum(stats['plugin_crashes'] / sum(stats->>'usage_hours') FROM aggregates
WHERE dimensions->>'channel' = 'nightly' AND activity_date = '2016-03-14'
```

Main process crashes by build date and E10S cohort.

```sql
WITH channel_rates AS (
  SELECT dimensions['build_id'] AS build_id,
         SUM(stats['main_crashes']) AS main_crashes, -- total number of crashes
         SUM(stats['usage_hours']) / 1000 AS usage_kilohours, -- thousand hours of usage
         dimensions['e10s_cohort'] AS e10s_cohort -- e10s cohort
   FROM crash_aggregates
   WHERE dimensions['experiment_id'] is null -- not in an experiment
     AND regexp_like(dimensions['build_id'], '^\d{14}$') -- validate build IDs
     AND dimensions['build_id'] > '20160201000000' -- only in the date range that we care about
   GROUP BY dimensions['build_id'], dimensions['e10s_cohort']
)
SELECT parse_datetime(build_id, 'yyyyMMddHHmmss') as build_id, -- program build date
       usage_kilohours, -- thousands of usage hours
       e10s_cohort, -- e10s cohort
       main_crashes / usage_kilohours AS main_crash_rate -- crash rate being defined as crashes per thousand usage hours
FROM channel_rates
WHERE usage_kilohours > 100 -- only aggregates that have statistically significant usage hours
ORDER BY build_id ASC
```

Development
-----------

This project uses [Vagrant](https://www.vagrantup.com/) for setting up the development environment, and [Ansible](https://www.ansible.com/) for automation. You can install these pretty easily with `sudo apt-get install vagrant ansible` on Debian-derivative Linux distributions.

To set up a development environment, simply run `vagrant up` and then `vagrant ssh` in the project root folder. This will set up a development environment inside a virtual machine and SSH into it. Do `cd /vagrant` to get to the project folder within the VM.

To set up the environment locally on Ubuntu 14.04 LTS, simply run `sudo ansible-playbook -i ansible/inventory/localhost.ini ansible/dev.yml`.

Note that within the Vagrant VM, `~/miniconda2/bin/python` is the main Python binary (the one you get when you do `which python`). All the packages are installed for Miniconda's Python rather than the system Python.

To backfill data, just run `crash_rate_aggregates/crash_aggregator.py` with the desired start/end dates as the `--min-submission-date`/`--max-submission-date` arguments.

To run the tests, execute `python -m unittest discover -s /vagrant/test` in the Vagrant VM.

To add a new dimension to compare on, add matching entries to `COMPARABLE_DIMENSIONS` and `DIMENSION_NAMES` in `crash_rate_aggregates/crash_aggregator.py`. Also make sure to add a matching entry to `ping_dimensions` in `test/dataset.py`, and update the counts in the relevant tests.

Note that new dimensions will exponentially increase the number of aggregates. Adding one Boolean aggregate, for example, might double the possibilities.

When running in the Telemetry Analysis Environment, debugging can be a pain. These commands can be helpful:

* `yum install w3m; w3m http://localhost:4040` to show the Spark UI in the command line.
* `tail -f /mnt/spark.log` to view the Spark logs.
* `yum install nethogs; sudo nethogs` to monitor network utilization for each process.

To run the job, see `/vagrant/crash_rate_aggregates/crash_aggregator.py --help`. The simplest possible usage is `python crash_aggregator.py`.

Deployment
----------

1. Install [Ansible Playbook](http://docs.ansible.com/ansible/playbooks.html): `sudo apt-get install ansible`.
2. Set up the scheduled Spark analysis on the [Telemetry analysis service](https://analysis.telemetry.mozilla.org/cluster/schedule) to run daily, using the `run_crash_aggregator.ipynb` Jupyter notebook.
