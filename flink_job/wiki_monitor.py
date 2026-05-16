"""
Real-time Wikipedia Edit Monitoring Pipeline
"""

import json
import logging
import boto3
import time

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STREAM_NAME = "wiki-events"
REGION = "ap-south-1"
WINDOW_SIZE_SECONDS = 60
ANOMALY_THRESHOLD = 20


def parse_event(json_str):
    try:
        e = json.loads(json_str)
        if e.get("type") not in ("edit", "new"):
            return None
        if e.get("namespace") != 0:
            return None
        return (e.get("wiki", "unknown"), e.get("title", "untitled"),
                e.get("user", "anonymous"), 1 if e.get("bot") else 0)
    except Exception:
        return None


def count_edits(a, b):
    return (a[0], a[1] + b[1], a[2] + b[2])


def count_page_edits(a, b):
    return (a[0], a[1], a[2] + b[2])


_dynamo = None

def get_dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.client("dynamodb", region_name=REGION)
    return _dynamo


def _current_window_ts():
    return str(int(time.time() // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS)


def write_edit_metric(record):
    wiki, total, bots = record
    try:
        get_dynamo().put_item(
            TableName="wiki-edit-metrics",
            Item={
                "wiki": {"S": wiki},
                "window_end": {"S": _current_window_ts()},
                "total_edits": {"N": str(total)},
                "bot_edits": {"N": str(bots)},
                "human_edits": {"N": str(total - bots)},
                "bot_ratio": {"N": str(round(bots / total, 3) if total else 0)},
            },
        )
    except Exception as e:
        logger.error(f"Failed writing edit_metric: {e}")
    return record


def write_anomaly_if_needed(record):
    page, wiki, count = record
    if count < ANOMALY_THRESHOLD:
        return record
    try:
        get_dynamo().put_item(
            TableName="wiki-anomalies",
            Item={
                "page_title": {"S": page},
                "wiki": {"S": wiki},
                "window_end": {"S": _current_window_ts()},
                "edit_count": {"N": str(count)},
                "severity": {"S": "high" if count > 50 else "medium"},
            },
        )
    except Exception as e:
        logger.error(f"Failed writing anomaly: {e}")
    return record


def write_page_count(record):
    page, wiki, count = record
    try:
        get_dynamo().put_item(
            TableName="wiki-top-pages",
            Item={
                "page_title": {"S": page},
                "wiki": {"S": wiki},
                "window_end": {"S": _current_window_ts()},
                "edit_count": {"N": str(count)},
            },
        )
    except Exception as e:
        logger.error(f"Failed writing page_count: {e}")
    return record


def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    kinesis_props = {
        "aws.region": REGION,
        "flink.stream.initpos": "LATEST",
    }
    
    source = FlinkKinesisConsumer(STREAM_NAME, SimpleStringSchema(), kinesis_props)
    raw_stream = env.add_source(source).name("kinesis-source")

    parsed = (
        raw_stream
        .map(parse_event)
        .filter(lambda x: x is not None)
        .name("parsed-events")
    )

    # Edit metrics by wiki
    (
        parsed
        .map(lambda x: (x[0], 1, x[3]))
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .reduce(count_edits)
        .map(write_edit_metric)
        .name("edit-metrics")
    )

    # Anomalies + Top pages
    page_counts = (
        parsed
        .map(lambda x: (x[1], x[0], 1))
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .reduce(count_page_edits)
    )

    page_counts.map(write_anomaly_if_needed).name("anomalies")
    page_counts.map(write_page_count).name("page-counts")

    env.execute("wiki-edit-monitor")


if __name__ == "__main__":
    build_pipeline()