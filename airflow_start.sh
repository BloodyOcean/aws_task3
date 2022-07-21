#!/bin/bash

echo $0
echo $1

# Install necessary packages
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip
sudo apt-get install openjdk-8-jre
sudo apt-get install awscli
sudo apt-get install cron
sudo apt-get install unzip
sudo apt-get install libmysqlclient-dev

# Install necessary dependecies
pip install mysqlclient
pip install mimesis
pip install boto3
pip install sqlalchemy
pip install sqlalchemy_utils
pip install pyspark

sudo mv $1 .

wget -P ./part2_spark https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

# Airflow needs a home. `~/airflow` is the default, but you can put it
# somewhere else if you prefer (optional)
export AIRFLOW_HOME=~/airflow

export PATH=/home/ubuntu/.local/bin:$PATH

source ~/.bash_profile

# Install Airflow using the constraints file
AIRFLOW_VERSION="2.3.3"
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Move dags from repository inside airflow root folder
sudo mv dags ~/airflow

# Set-up aws user configuration
aws configure

# The Standalone command will initialise the database, make a user,
# and start all components for you.
airflow standalone

# Visit localhost:8080 in the browser and use the admin account details
# shown on the terminal to login.
# Enable the example_bash_operator dag in the home page