Crash Rate Aggregates
=====================

Scheduled batch job for computing aggregate crash rates across a variety of criteria.

* A Telemetry batch job runs daily using a [scheduled analysis job](https://analysis.telemetry.mozilla.org/schedule).
  * The job, `crash-rate-aggregates`, runs the project from the `crash-rate-aggregates.tar.gz` tarball generated by Ansible.
  * The job takes its needed credentials from the telemetry-spark-emr-2 S3 bucket.
* A large Amazon RDB instance with Postgresql is filled by the analysis job.
* The database is meant to be consumed by [re:dash](https://sql.telemetry.mozilla.org/dashboard/general) to make crash rate dashboards.

Development
-----------

First, make sure you have your SPARK\_HOME environment variable set to the Apache Spark program directory. For example, `export SPARK_HOME=/home/anthony/Desktop/spark-1.6.0-bin-hadoop2.6`.

To run the tests, execute `python -m unittest discover` in the project directory.

Deployment
----------

1. Install Ansible Playbook and Boto: `sudo apt-get install software-properties-common python-boto; sudo apt-add-repository ppa:ansible/ansible; sudo apt-get update; sudo apt-get install ansible`.
2. Run `ansible-playbook ansible/deploy.yml` to set up the RDB instance.
3. Set up the scheduled task on the [Telemetry analysis service](https://analysis.telemetry.mozilla.org/schedule) to run daily, using the `crash-rate-aggregates.tar.gz` tarball generated by Ansible in step 2.