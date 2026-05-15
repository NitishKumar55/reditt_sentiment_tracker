#!/bin/bash
set -e

# Download the CORRECT Kinesis connector (DataStream API version)
KINESIS_JAR="/usr/lib/flink/lib/flink-connector-kinesis-1.18.1.jar"
if [ ! -f "$KINESIS_JAR" ]; then
    echo "Downloading Kinesis connector JAR..."
    sudo wget -O $KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kinesis/1.18.1/flink-connector-kinesis-1.18.1.jar
fi

# Install Python deps
sudo pip3 install boto3 --quiet

# Pull latest Flink code from S3
aws s3 cp s3://nitish-wiki-monitor-code/flink_job/wiki_monitor.py /home/hadoop/wiki_monitor.py

# Run Flink with JAR explicitly specified
flink run \
    --jarfile $KINESIS_JAR \
    -py /home/hadoop/wiki_monitor.py