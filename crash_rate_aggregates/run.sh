#!/usr/bin/env bash

<<ENDCOMMENT
# these setup routines are used to set up an environment when non is available, like in Vagrant
# since the script usually runs on the Telemetry Spark Analysis environment, the requirements should already be installed

# install packages
sudo apt-get install -y libsnappy-dev liblzma-dev openjdk-7-jdk
sudo pip2 install telemetry-tools
sudo apt-get install -y python-numpy python-pandas # these are dependencies of python_moztelemetry, but installing them with apt-get massively speeds up python_moztelemetry installation
sudo pip2 install python_moztelemetry

# install Spark
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.3.1-bin-hadoop2.4.tgz -O spark-hadoop.tgz
tar -xzf spark-hadoop.tgz
rm spark-hadoop.tgz
export SPARK_HOME=$(pwd)/spark-hadoop
export PYTHONPATH=$SPARK_HOME/python
ENDCOMMENT

# fill crash aggregates
python fill_database.py
