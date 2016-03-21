Crash Rate Aggregates
=====================

[![Build Status](https://travis-ci.org/Uberi/moz-crash-rate-aggregates.svg?branch=master)](https://travis-ci.org/Uberi/moz-crash-rate-aggregates)

Scheduled batch job for computing aggregate crash rates across a variety of criteria.

* A Spark analysis runs daily using a [scheduled analysis job](https://analysis.telemetry.mozilla.org/cluster/schedule).
    * The job, `crash-aggregates`, runs the `run_fill_database.ipynb` Jupyter notebook, which downloads, installs, and runs the crash-rate-aggregates on the cluster.
    * The job takes its needed credentials from the telemetry-spark-emr-2 S3 bucket.
    * Currently, this job is running under :azhang's account every day at 11pm UTC, with the default settings for everything else. The job is named `crash-aggregates`.
* A large Amazon RDB instance with Postgresql is filled by the analysis job.
    * Currently, this instance is available at `crash-rate-aggregates.cppmil15uwvg.us-west-2.rds.amazonaws.com:5432`, and credentials are available on S3.
* The database is meant to be consumed by [re:dash](https://sql.telemetry.mozilla.org/dashboard/general) to make crash rate dashboards.
    * On the re:dash interface, these aggregates can be used by queries when the query data source is set to "Crash-DB".
    * The database has one table, `crash_aggregates`, that has all of the dimensions, submission dates, and etc.
    * For example, you can get the main process crashes per hour on Nightly for March 14, 2016 with `SELECT sum(stats->>'main_crashes') / sum(stats->>'usage_hours') FROM aggregates WHERE dimensions->>'channel' = 'nightly' AND submission_date = '2016-03-14'`.

Development
-----------

This project uses [Vagrant](https://www.vagrantup.com/) for setting up the development environment, and [Ansible](https://www.ansible.com/) for automation. You can install these pretty easily with `sudo apt-get install vagrant ansible` on Debian-derivatives.

To set up a development environment, simply run `vagrant up` and then `vagrant ssh` in the project root folder. This will open a terminal within the Vagrant VM. Do `cd /vagrant` to get to the project folder within the VM.

To set up the environment locally on Ubuntu 14.04 LTS, simply run `sudo ansible-playbook -i ansible/inventory/localhost.ini ansible/dev.yml`.

Note that within the Vagrant VM, you should use `~/miniconda2/bin/python` as the main Python binary. All the packages are installed for Miniconda's Python rather than the system Python.

To backfill data, just run `crash_rate_aggregates/fill_database.py` with the desired start/end dates as the `--min-submission-date`/`--max-submission-date` arguments. The operation is idempotent and existing aggregates for those dates are overwritten.

To run the tests, execute `PYTHONPATH=.:$PYTHONPATH python -m unittest discover -s test` in the project root directory.

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
