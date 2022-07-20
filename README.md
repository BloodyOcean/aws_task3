# Overview
This program generates transactions, loads them to RDS and processes using Spark and downloads partitioned parquet to S3
# Installation
1) Please, copy or download this repository using `wget` or `git copy`
2) **dont forget to update your apt-get using `sudo apt-get update`**
3) Make bash script executable using `sudo chmmode +x crontab_start.sh`
4) Run bash script using `./crontab_start.sh`
