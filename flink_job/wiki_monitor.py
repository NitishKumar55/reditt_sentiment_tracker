"""
Real-time Wikipedia Edit Monitoring Pipeline

Reads Wikipedia edit events from Kinesis and produces 3 outputs:
  1. Edit volume metrics: edits per minute by wiki, with bot ratio
  2. Anomaly detection: pages getting > 20 edits / minute
  3. Top-edited pages per window
"""

import json
import logging
import boto3

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kinesis import FlinkKinesisConsumer
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common.time import Time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
STREAM_NAME = "wikipedia_que"
REGION = "ap-south-1"
WINDOW_SIZE_SECONDS = 60
ANOMALY_THRESHOLD = 20
TOP_N_PAGES = 10


# ─────────────────────────────────────────────────────────────
# Parsing
# ─────────────────────────────────────────────────────────────
def parse_event(json_str):
    """Parse incoming Kinesis record. Returns None if invalid/uninteresting."""
    try:
        e = json.loads(json_str)
        
        if e.get("type") not in ("edit", "new"):
            return None
        if e.get("namespace") != 0:
            return None
        
        wiki = e.get("wiki", "unknown")
        page = e.get("title", "untitled")
        user = e.get("user", "anonymous")
        is_bot = 1 if e.get("bot") else 0
        
        return (wiki, page, user, is_bot)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# Aggregation functions (reduce-style)
# ─────────────────────────────────────────────────────────────
def count_edits(a, b):
    """Reduce: sum total and bot counts within a window."""
    # tuple format: (wiki, total_count, bot_count)
    return (a[0], a[1] + b[1], a[2] + b[2])


def count_page_edits(a, b):
    """Reduce: sum edit counts per page."""
    # tuple format: (page, wiki, count)
    return (a[0], a[1], a[2] + b[2])


# ─────────────────────────────────────────────────────────────
# DynamoDB sink (called from map operators)
# ─────────────────────────────────────────────────────────────
_dynamo = None

def get_dynamo():
    global _dynamo
    if _dynamo is None:
        _dynamo = boto3.client("dynamodb", region_name=REGION)
    return _dynamo


def write_edit_metric(record):
    """record = (wiki, total_edits, bot_edits)"""
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
    """record = (page, wiki, count)"""
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
        logger.info(f"ANOMALY: {page} got {count} edits")
    except Exception as e:
        logger.error(f"Failed writing anomaly: {e}")
    return record


def write_page_count(record):
    """record = (page, wiki, count) — write all page counts; top-N can be queried later."""
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


def _current_window_ts():
    """Helper to get current minute as a window-end approximation."""
    import time
    return str(int(time.time() // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS)


# ─────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────
def build_pipeline():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)

    # Kinesis source
    kinesis_props = {
        "aws.region": REGION,
        "flink.stream.initpos": "LATEST",
    }
    source = FlinkKinesisConsumer(STREAM_NAME, SimpleStringSchema(), kinesis_props)
    raw_stream = env.add_source(source).name("kinesis-source")

    # Parse and filter once, reuse for all outputs
    parsed = (
        raw_stream
        .map(parse_event)
        .filter(lambda x: x is not None)
        .name("parsed-events")
    )

    # ───────────────────────────────────────
    # Output 1: Edit metrics by wiki
    # ───────────────────────────────────────
    (
        parsed
        .map(lambda x: (x[0], 1, x[3]))  # (wiki, 1, is_bot)
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .reduce(count_edits)
        .map(write_edit_metric)
        .name("edit-metrics")
    )

    # ───────────────────────────────────────
    # Output 2: Anomalies + Top pages
    # Both use the same per-page count, so compute once
    # ───────────────────────────────────────
    page_counts = (
        parsed
        .map(lambda x: (x[1], x[0], 1))  # (page, wiki, 1)
        .key_by(lambda x: x[0])
        .window(TumblingProcessingTimeWindows.of(Time.seconds(WINDOW_SIZE_SECONDS)))
        .reduce(count_page_edits)
    )

    # Anomalies: flag if count > threshold
    page_counts.map(write_anomaly_if_needed).name("anomalies")

    # Top pages: write all counts, query top-N later from DynamoDB
    page_counts.map(write_page_count).name("page-counts")

    env.execute("wiki-edit-monitor")


if __name__ == "__main__":
    build_pipeline()