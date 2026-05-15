#!/bin/bash
set -e

echo "Downloading Flink code from S3..."
aws s3 cp s3://nitish-wiki-monitor-code/flink_job/wiki_monitor.py /home/hadoop/wiki_monitor.py

echo "Installing Python deps..."
sudo pip3 install boto3 apache-flink==1.18.1

echo "Downloading Kinesis connector JAR..."
sudo wget -O /usr/lib/flink/lib/flink-sql-connector-kinesis-1.18.1.jar \
  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kinesis/1.18.1/flink-sql-connector-kinesis-1.18.1.jar || true

echo "Submitting Flink job..."
flink run -py /home/hadoop/wiki_monitor.py

echo "Done."