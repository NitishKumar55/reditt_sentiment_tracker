"""
Real-time Wikipedia Edit Monitor on AWS Managed Flink 1.20
Using shaded fat JAR (built by Maven, contains all connector dependencies)
"""

import os
import logging
from pyflink.table import EnvironmentSettings, TableEnvironment

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAM_NAME = "wikipedia_que"
REGION = "ap-south-1"
ACCOUNT_ID = "141552609063"
STREAM_ARN = f"arn:aws:kinesis:{REGION}:{ACCOUNT_ID}:stream/{STREAM_NAME}"


def main():
    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)

    # The fat JAR is loaded via the 'jarfile' runtime property in Managed Flink config.
    # No pipeline.jars needed — everything is shaded into one JAR.

    # Source: Kinesis stream
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
            'stream.arn' = '{STREAM_ARN}',
            'aws.region' = '{REGION}',
            'source.init.position' = 'LATEST',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # Sink 1: Edit metrics
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
            'aws.region' = '{REGION}'
        )
    """)

    # Sink 2: Top pages
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
            'aws.region' = '{REGION}'
        )
    """)

    # Sink 3: Anomalies
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
            'aws.region' = '{REGION}'
        )
    """)

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
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

    statement_set.add_insert_sql("""
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

    statement_set.add_insert_sql("""
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

    statement_set.execute()


if __name__ == "__main__":
    main()