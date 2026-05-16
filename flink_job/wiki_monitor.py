"""
Real-time Wikipedia Edit Monitor on AWS Managed Flink
"""

import logging
import json
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Read config from environment (Managed Flink sets these)
APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

# Defaults — Managed Flink overrides these from app properties
STREAM_NAME = os.environ.get("STREAM_NAME", "wikipedia_que")
REGION = os.environ.get("REGION", "ap-south-1")


def get_application_properties():
    """Read properties Managed Flink injects."""
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as f:
            contents = f.read()
            properties = json.loads(contents)
            return properties
    return {}


def property_map(props, key):
    for prop in props:
        if prop.get("PropertyGroupId") == key:
            return prop.get("PropertyMap", {})
    return {}


def main():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # Add JARs from lib directory (Managed Flink uses this convention)
    t_env.get_config().set(
        "pipeline.jars",
        "file:///opt/flink/usrlib/flink-sql-connector-kinesis-4.2.0-1.18.jar;"
        "file:///opt/flink/usrlib/flink-sql-connector-dynamodb-4.2.0-1.18.jar"
    )

    # Get config from Managed Flink
    props = get_application_properties()
    kinesis_props = property_map(props, "KinesisSource")
    stream_name = kinesis_props.get("stream.name", STREAM_NAME)
    region = kinesis_props.get("aws.region", REGION)

    # ─────────────────────────────────────
    # Source: Kinesis (Wikipedia events)
    # ─────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE wiki_events (
            id BIGINT,
            wiki STRING,
            type STRING,
            title STRING,
            `user` STRING,
            bot BOOLEAN,
            `namespace` INT,
            `timestamp` BIGINT,
            event_time AS PROCTIME()
        ) WITH (
            'connector' = 'kinesis',
            'stream' = '{stream_name}',
            'aws.region' = '{region}',
            'scan.stream.initpos' = 'LATEST',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # ─────────────────────────────────────
    # Sink 1: Edit metrics per wiki to DynamoDB
    # ─────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE wiki_edit_metrics_sink (
            wiki STRING,
            window_end STRING,
            total_edits BIGINT,
            bot_edits BIGINT,
            human_edits BIGINT,
            PRIMARY KEY (wiki, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'dynamodb',
            'table-name' = 'wiki-edit-metrics',
            'aws.region' = '{region}'
        )
    """)

    # Compute edit metrics: 1-minute tumbling windows by wiki
    t_env.execute_sql("""
        INSERT INTO wiki_edit_metrics_sink
        SELECT
            wiki,
            CAST(window_end AS STRING) AS window_end,
            COUNT(*) AS total_edits,
            SUM(CASE WHEN bot THEN 1 ELSE 0 END) AS bot_edits,
            COUNT(*) - SUM(CASE WHEN bot THEN 1 ELSE 0 END) AS human_edits
        FROM TABLE(
            TUMBLE(TABLE wiki_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        WHERE type IN ('edit', 'new') AND `namespace` = 0
        GROUP BY wiki, window_start, window_end
    """)

    # ─────────────────────────────────────
    # Sink 2: Top pages per window to DynamoDB
    # ─────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE wiki_top_pages_sink (
            page_title STRING,
            wiki STRING,
            window_end STRING,
            edit_count BIGINT,
            PRIMARY KEY (page_title, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'dynamodb',
            'table-name' = 'wiki-top-pages',
            'aws.region' = '{region}'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO wiki_top_pages_sink
        SELECT
            title AS page_title,
            wiki,
            CAST(window_end AS STRING) AS window_end,
            COUNT(*) AS edit_count
        FROM TABLE(
            TUMBLE(TABLE wiki_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        WHERE type IN ('edit', 'new') AND `namespace` = 0
        GROUP BY title, wiki, window_start, window_end
    """)

    # ─────────────────────────────────────
    # Sink 3: Anomalies (pages with >20 edits/min)
    # ─────────────────────────────────────
    t_env.execute_sql(f"""
        CREATE TABLE wiki_anomalies_sink (
            page_title STRING,
            wiki STRING,
            window_end STRING,
            edit_count BIGINT,
            severity STRING,
            PRIMARY KEY (page_title, window_end) NOT ENFORCED
        ) WITH (
            'connector' = 'dynamodb',
            'table-name' = 'wiki-anomalies',
            'aws.region' = '{region}'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO wiki_anomalies_sink
        SELECT
            title AS page_title,
            wiki,
            CAST(window_end AS STRING) AS window_end,
            COUNT(*) AS edit_count,
            CASE 
                WHEN COUNT(*) > 50 THEN 'high'
                ELSE 'medium'
            END AS severity
        FROM TABLE(
            TUMBLE(TABLE wiki_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE)
        )
        WHERE type IN ('edit', 'new') AND `namespace` = 0
        GROUP BY title, wiki, window_start, window_end
        HAVING COUNT(*) > 20
    """)


if __name__ == "__main__":
    main()