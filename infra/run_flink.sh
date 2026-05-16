#!/bin/bash
set -e

LIB_DIR="/usr/lib/flink/lib"

# Kinesis DataStream connector (this URL works)
KINESIS_JAR="$LIB_DIR/flink-connector-kinesis-1.18.1.jar"
if [ ! -f "$KINESIS_JAR" ]; then
    echo "Downloading Kinesis DataStream connector..."
    sudo wget -O $KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kinesis/1.18.1/flink-connector-kinesis-1.18.1.jar
fi

# AWS Kinesis SQL connector (correct path)
SQL_KINESIS_JAR="$LIB_DIR/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar"
if [ ! -f "$SQL_KINESIS_JAR" ]; then
    echo "Downloading Kinesis SQL connector..."
    sudo wget -O $SQL_KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-aws-kinesis-streams/4.2.0-1.18/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar
fi

# Install Python deps
sudo pip3 install boto3 --quiet

# Pull latest Flink code from S3
echo "Downloading latest Flink job code..."
aws s3 cp s3://nitish-wiki-monitor-code/flink_job/wiki_monitor.py /home/hadoop/wiki_monitor.py

# Run Flink
echo "Submitting Flink job..."
flink run \
    --jarfile $KINESIS_JAR \
    -py /home/hadoop/wiki_monitor.py