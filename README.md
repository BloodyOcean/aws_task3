# Overview
This program generates transactions, loads them to RDS and processes using Spark and downloads partitioned parquet to S3. 
All theese processess use `crontab` for regular work
# Installation
1) Please, copy or download this repository using `wget` or `git copy`
2) **dont forget to update your apt-get using `sudo apt-get update`**
3) Make bash script executable using `sudo chmmod +x crontab_start.sh`
4) Run bash script using `./crontab_start.sh <path to aws credentials>`
# Warning
1) Your machine should have specific aws credentials file and access key for propper work
2) Dont forget to pass path to the credentials before executing script
3) If you are using zip archive of repository. Please, dont forget to install `unzip` using `sudo apt-get install unzip`
