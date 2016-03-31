Crash Rate Aggregates
=====================

[![Build Status](https://travis-ci.org/Uberi/moz-crash-rate-aggregates.svg?branch=master)](https://travis-ci.org/Uberi/moz-crash-rate-aggregates)

Scheduled batch job for computing aggregate crash rates across a variety of criteria.

* A Spark analysis runs daily using a [scheduled analysis job](https://analysis.telemetry.mozilla.org/cluster/schedule).
    * The job, `crash-aggregates`, runs the `run_fill_database.ipynb` Jupyter notebook, which downloads, installs, and runs the crash-rate-aggregates on the cluster.
    * The job takes its needed credentials from the telemetry-spark-emr-2 S3 bucket.
    * Currently, this job is running under :azhang's account every day at 11pm UTC, with the default settings for everything else. The job is named `crash-aggregates`.
* A large Amazon RDB instance with Postgresql is filled by the analysis job.
    * Currently, this instance is available at `crash-rate-aggregates.cppmil15uwvg.us-west-2.rds.amazonaws.com:5432`, and credentials are available on S3 under `telemetry-spark-emr-2/crash_rate_aggregates_credentials`.

The Database
------------

The database is meant to be consumed by [re:dash](https://sql.telemetry.mozilla.org/dashboard/general) to make crash rate dashboards.

In the re:dash interface, make a new query, and set the data source to "Crash-DB". Running the query then runs it against the crash aggregates database.

The database has a pretty simple schema:

```sql
CREATE TABLE IF NOT EXISTS aggregates (
    id serial PRIMARY KEY,
    submission_date date NOT NULL,
    activity_date date NOT NULL,
    dimensions jsonb,
    stats jsonb
);
```

* Each row represents a single aggregate - a single, disjoint subpopulation, identified by `submission_date`, `activity_date`, and `dimensions`.
* `submission_date` is the date the pings were received for this aggregate. It's also what we partition the tables based on, so queries that limit this field to a small range will be significantly more efficient.
* `activity_date` is the date that pings were created for this aggregate, on the client side. This is useful since it's also the time that the measurements in the ping were taken.
* `dimensions` contains all of the other dimensions we distinguish by:
    * `dimensions->>'build_version'` is the program version, like `46.0a1`.
    * `dimensions->>'build_date'` is the YYYYMMDDhhmmss timestamp the program was built, like `20160123180541`.
    * `dimensions->>'channel'` is the channel, like `release` or `beta`.
    * `dimensions->>'application'` is the program name, like `Firefox` or `Fennec`.
    * `dimensions->>'os_name'` is the name of the OS the program is running on, like `Darwin` or `Windows_NT`.
    * `dimensions->>'os_version'` is the version of the OS the program is running on.
    * `dimensions->>'architecture'` is the architecture that the program was built for (not necessarily the one it is running on).
    * `dimensions->>'country'` is the country code for the user (determined using geoIP), like `US` or `UK`.
    * `dimensions->>'experiment_id'` is the identifier of the experiment being participated in, such as `e10s-beta46-noapz@experiments.mozilla.org`, or null if no experiment.
    * `dimensions->>'experiment_branch'` is the branch of the experiment being participated in, such as `control` or `experiment`, or null if no experiment.
    * `dimensions->>'e10s_enabled'` is whether E10S is enabled.
    * All of the above fields can potentially be null.
* `stats` contains the aggregate values that we care about:
    * `stats->>'usage_hours'` is the number of user-hours represented by the aggregate.
    * `stats->>'main_crashes'` is the number of main process crashes represented by the aggregate (or just program crashes, in the non-E10S case).
    * `stats->>'content_crashes'` is the number of content process crashes represented by the aggregate.
    * `stats->>'plugin_crashes'` is the number of plugin process crashes represented by the aggregate.

Plugin process crashes per hour on Nightly for March 14:

```sql
SELECT sum(stats->>'plugin_crashes') / sum(stats->>'usage_hours') FROM aggregates
WHERE dimensions->>'channel' = 'nightly' AND submission_date = '2016-03-14'
```

Main process crashes by date and E10S setting.

```sql
WITH channel_rates AS (
  SELECT dimensions->>'build_date' AS build_date,
         SUM((stats->>'main_crashes')::float) AS main_crashes, -- total number of crashes
         SUM((stats->>'usage_hours')::float) / 1000 AS usage_kilohours, -- thousand hours of usage
         dimensions->>'e10s_enabled' AS e10s_enabled -- e10s setting
   FROM aggregates
   WHERE (dimensions->>'experiment_id') IS NULL -- not in an experiment
     AND dimensions->>'build_date' ~ '^\d{14}$' -- validate build IDs
     AND dimensions->>'build_date' > '20160201000000' -- only in the date range that we care about
   GROUP BY dimensions->>'build_date', dimensions->>'e10s_enabled'
)
SELECT to_date(build_date, 'YYYYMMDDHH24MISS'), -- program build date
       usage_kilohours, -- thousands of usage hours
       e10s_enabled, -- e10s setting
       main_crashes / usage_kilohours AS main_crash_rate -- crash rate being defined as crashes per thousand usage hours
FROM channel_rates
WHERE usage_kilohours > 100 -- only aggregates that have statistically significant usage hours
ORDER BY build_date ASC
```

Development
-----------

This project uses [Vagrant](https://www.vagrantup.com/) for setting up the development environment, and [Ansible](https://www.ansible.com/) for automation. You can install these pretty easily with `sudo apt-get install vagrant ansible` on Debian-derivatives.

To set up a development environment, simply run `vagrant up` and then `vagrant ssh` in the project root folder. This will open a terminal within the Vagrant VM. Do `cd /vagrant` to get to the project folder within the VM.

To set up the environment locally on Ubuntu 14.04 LTS, simply run `sudo ansible-playbook -i ansible/inventory/localhost.ini ansible/dev.yml`.

Note that within the Vagrant VM, you should use `~/miniconda2/bin/python` as the main Python binary. All the packages are installed for Miniconda's Python rather than the system Python.

To backfill data, just run `crash_rate_aggregates/fill_database.py` with the desired start/end dates as the `--min-submission-date`/`--max-submission-date` arguments. The operation is idempotent and existing aggregates for those dates are overwritten.

To run the tests, execute `PYTHONPATH=.:$PYTHONPATH python -m unittest discover -s /vagrant/test` in the Vagrant VM.

To add a new dimension to compare on, add matching entries to `COMPARABLE_DIMENSIONS` and `DIMENSION_NAMES` in `crash_rate_aggregates/fill_database.py`.

When running in the Telemetry Analysis Environment, debugging can be a pain. These commands can be helpful:

* `yum install w3m; w3m http://localhost:4040` to show the Spark UI in the command line.
* `tail -f /mnt/spark.log` to view the Spark logs.
* `yum install nethogs; sudo nethogs` to monitor network utilization for each process.

`crash_rate_aggregates/run.sh` is useful for manually setting up running the crash rate aggregator for the current day. To use this script, AWS credentials need to be set up. This can be done by running `aws configure` in the Vagrant VM, and following the prompts.

Deployment
----------

1. Install [Ansible Playbook](http://docs.ansible.com/ansible/playbooks.html) and Boto: `sudo apt-get install software-properties-common python-boto; sudo apt-add-repository ppa:ansible/ansible; sudo apt-get update; sudo apt-get install ansible`.
2. Run `ansible-playbook ansible/deploy.yml` to set up the RDB instance on AWS.
3. Set up the scheduled Spark analysis on the [Telemetry analysis service](https://analysis.telemetry.mozilla.org/cluster/schedule) to run daily, using the `run_fill_database.ipynb` Jupyter notebook.
