import json
import time
import boto3
import requests
from sseclient import SSEClient

# Configuration
WIKI_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
STREAM_NAME = "wikipedia_que "
REGION = "ap-south-1"
LANGUAGES = ["en"]  # only English Wikipedia, change to ["en", "hi"] for more
USER_AGENT = "wiki-trend-engine/0.1 (nitish.k0596@gmail.com)"

# boto3 reads credentials from IAM role on EC2 automatically
kinesis = boto3.client("kinesis", region_name=REGION)


def extract_event_data(event_json):
    """Pull useful fields from a Wikipedia change event."""
    return {
        "event_id": event_json.get("id"),
        "wiki": event_json.get("wiki"),
        "type": event_json.get("type"),
        "title": event_json.get("title"),
        "user": event_json.get("user"),
        "bot": event_json.get("bot", False),
        "timestamp": event_json.get("timestamp"),
        "namespace": event_json.get("namespace"),
        "comment": event_json.get("comment", "")[:200],
        "server_name": event_json.get("server_name"),
        "minor": event_json.get("minor", False),
        "length_old": event_json.get("length", {}).get("old"),
        "length_new": event_json.get("length", {}).get("new"),
    }


def push_to_kinesis(event_data):
    """Send a single event to Kinesis stream."""
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(event_data),
            PartitionKey=event_data["wiki"] or "default",
        )
        return response["SequenceNumber"]
    except Exception as e:
        print(f"Failed to push to Kinesis: {e}")
        return None


def stream_events():
    """Connect to Wikipedia SSE stream and yield events."""
    headers = {"User-Agent": USER_AGENT}
    print(f"Connecting to Wikipedia stream: {WIKI_STREAM_URL}")
    response = requests.get(WIKI_STREAM_URL, stream=True, headers=headers)
    client = SSEClient(response)
    
    for event in client.events():
        if event.event != "message":
            continue
        try:
            yield json.loads(event.data)
        except json.JSONDecodeError:
            continue


def main():
    print(f"Producer started → AWS Kinesis ({REGION})")
    print(f"Stream: {STREAM_NAME}")
    print(f"Filtering languages: {LANGUAGES}\n")
    
    pushed_count = 0
    skipped_count = 0
    last_status_time = time.time()
    
    while True:
        try:
            for event in stream_events():
                # Filter to specific Wikipedia languages
                wiki = event.get("wiki", "")
                wiki_lang = wiki.replace("wiki", "")
                
                if wiki_lang not in LANGUAGES:
                    skipped_count += 1
                    continue
                
                # Skip bot edits (optional - comment out if you want them)
                if event.get("bot", False):
                    skipped_count += 1
                    continue
                
                event_data = extract_event_data(event)
                seq = push_to_kinesis(event_data)
                
                if seq:
                    pushed_count += 1
                    print(f"[{event_data['wiki']}] {event_data['type']}: {event_data['title']}")
                
                # Print status every 30 seconds
                if time.time() - last_status_time > 30:
                    print(f"\n>>> Status: {pushed_count} pushed, {skipped_count} skipped\n")
                    last_status_time = time.time()
        
        except requests.exceptions.RequestException as e:
            print(f"Connection error: {e}. Reconnecting in 10s...")
            time.sleep(10)
        except Exception as e:
            print(f"Unexpected error: {e}. Reconnecting in 10s...")
            time.sleep(10)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped by user.")