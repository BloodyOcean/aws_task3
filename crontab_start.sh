#!/bin/bash

echo $0
echo $1

#install necessary packages
sudo apt-get update
sudo apt-get install python3
sudo apt-get install python3-pip
sudo apt-get install openjdk-8-jre
sudo apt-get install awscli
sudo apt-get install cron
sudo apt-get install unzip
sudo apt-get install libmysqlclient-dev

#create folder for crontab logs
mkdir /tmp/crontab_logs

#install necessary dependecies
pip install mimesis
pip install boto3
pip install sqlalchemy
pip install sqlalchemy_utils
pip install pyspark
pip install mysqlclient

sudo mv $1 .

wget -P ./part2_spark https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar

#set-up aws user configuration
aws configure

#write out current crontab
crontab -l > mycron

#echo new crons into cron file
echo "20 * * * * /usr/bin/python3 /home/ubuntu/aws_task3/part2_spark/main.py >> /tmp/crontab_logs/spark-`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1" >> mycron
echo "*/30 * * * * /usr/bin/python3 /home/ubuntu/aws_task3/part1_db/main.py --transactions >> /tmp/crontab_logs/transactions-`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1" >> mycron
echo "0 * * * * /usr/bin/python3 /home/ubuntu/aws_task3/part1_db/main.py --peoplecards 10 >> /tmp/crontab_logs/peoplecards-`date +\%Y\%m\%d\%H\%M\%S`.log 2>&1" >> mycron

#install new cron file
crontab mycron
rm mycron