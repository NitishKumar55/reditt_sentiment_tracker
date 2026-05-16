#!/bin/bash
set -e

LIB_DIR="/usr/lib/flink/lib"

# AWS Kinesis DataStream connector (the actual one that exists)
KINESIS_JAR="$LIB_DIR/flink-connector-aws-kinesis-streams-4.2.0-1.18.jar"

if [ -f "$KINESIS_JAR" ] && [ ! -s "$KINESIS_JAR" ]; then
    sudo rm -f $KINESIS_JAR
fi
if [ ! -f "$KINESIS_JAR" ]; then
    echo "Downloading Kinesis DataStream connector..."
    sudo wget --tries=3 --timeout=30 -O $KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-aws-kinesis-streams/4.2.0-1.18/flink-connector-aws-kinesis-streams-4.2.0-1.18.jar
    
    if [ ! -s "$KINESIS_JAR" ]; then
        echo "ERROR: Download failed"
        sudo rm -f $KINESIS_JAR
        exit 1
    fi
fi

# Also need the SQL connector for the wrapper class
SQL_KINESIS_JAR="$LIB_DIR/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar"
if [ ! -f "$SQL_KINESIS_JAR" ] || [ ! -s "$SQL_KINESIS_JAR" ]; then
    sudo rm -f $SQL_KINESIS_JAR
    sudo wget --tries=3 --timeout=30 -O $SQL_KINESIS_JAR \
        https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-aws-kinesis-streams/4.2.0-1.18/flink-sql-connector-aws-kinesis-streams-4.2.0-1.18.jar
fi

echo "Flink lib directory contents:"
ls -la $LIB_DIR/flink-*kinesis* 2>/dev/null

sudo pip3 install boto3 --quiet

aws s3 cp s3://nitish-wiki-monitor-code/flink_job/wiki_monitor.py /home/hadoop/wiki_monitor.py

echo "Submitting Flink job..."
flink run \
    --jarfile $KINESIS_JAR \
    -py /home/hadoop/wiki_monitor.py