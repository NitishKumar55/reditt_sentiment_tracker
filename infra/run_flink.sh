#!/bin/bash
set -e

LIB_DIR="/usr/lib/flink/lib"

# Kinesis SQL connector (this URL we know works)
SQL_KINESIS_JAR="$LIB_DIR/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar"

if [ -f "$SQL_KINESIS_JAR" ] && [ ! -s "$SQL_KINESIS_JAR" ]; then
    sudo rm -f $SQL_KINESIS_JAR
fi
if [ ! -f "$SQL_KINESIS_JAR" ]; then
    echo "Downloading Kinesis SQL connector..."
    sudo wget --tries=3 --timeout=30 -O $SQL_KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-aws-kinesis-streams/4.2.0-1.18/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar
fi

# Verify
echo "Flink lib directory contents:"
ls -la $LIB_DIR/flink-*kinesis* 2>/dev/null

# Install Python deps
sudo pip3 install boto3 --quiet

# Pull latest Flink code from S3
echo "Downloading latest Flink job code..."
aws s3 cp s3://nitish-wiki-monitor-code/flink_job/wiki_monitor.py /home/hadoop/wiki_monitor.py

# Run Flink — pass SQL connector since DataStream one doesn't exist
echo "Submitting Flink job..."
flink run \
    --jarfile $SQL_KINESIS_JAR \
    -py /home/hadoop/wiki_monitor.py