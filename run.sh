#!/usr/bin/env bash
echo "BASH_VERSION:" $BASH_VERSION

#################### Start Job ##################
startDatetime=$(date +%T)
echo "|> startDatetime -> ${startDatetime}"
startTime=$(date -d "${startDatetime}" +%s)

script=$1
case=${script%.*}

./bin/spark-submit --jars ./jars/jnr-posix-3.1.11.jar,./jars/hadoop-aws-3.3.1.jar,./jars/aws-java-sdk-bundle-1.12.344.jar,./jars/guava-31.1-jre.jar,./jars/guava-19.0.jar,./jars/spark-cassandra-connector-assembly_2.12-3.3.0.jar --packages com.datastax.spark:spark-cassandra-connector_2.12:3.3.0 pt_aws_cass.py > pt.log

#################### Job Ends ##################
endDatetime=$(date +%T)

echo "|> endDatetime -> ${endDatetime}"
endTime=$(date -d "${endDatetime}" +%s)


#################### Calculate Runtime ##################
diffSeconds="$(($endTime-$startTime))"

echo "Runtime for $case in Seconds: $diffSeconds"


diffTime=$(date -d @${diffSeconds} +"%H:%M:%S" -u)
echo "$case $startDatetime  Run time(H:M:S): $diffTime" >> Results.txt

