import requests
import time
import json
import boto3

SUBREDDITS = ["technology", "worldnews", "programming"]
POLL_INTERVAL = 30
USER_AGENT = "python:reddit-trend-engine:v0.1 (by /u/your_username)"
STREAM_NAME = "reddit-events"
REGION = "ap-south-1"

# boto3 reads credentials from ~/.aws/credentials automatically
kinesis = boto3.client("kinesis", region_name=REGION)

seen_post_ids = set()


def fetch_new_posts(subreddit):
    url = f"https://www.reddit.com/r/{subreddit}/new.json?limit=25"
    headers = {"User-Agent": USER_AGENT}
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        if response.status_code == 429:
            print(f"Rate limited on {subreddit}. Backing off 60s...")
            time.sleep(60)
            return None
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching {subreddit}: {e}")
        return None


def extract_post_data(post_json):
    data = post_json["data"]
    return {
        "post_id": data["id"],
        "subreddit": data["subreddit"],
        "title": data["title"],
        "author": data["author"],
        "score": data["score"],
        "num_comments": data["num_comments"],
        "created_utc": data["created_utc"],
        "url": data["url"],
        "selftext": data.get("selftext", "")[:500],
    }


def push_to_kinesis(post_data):
    try:
        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(post_data),
            PartitionKey=post_data["subreddit"],
        )
        return response["SequenceNumber"]
    except Exception as e:
        print(f"Failed to push: {e}")
        return None


def main():
    print(f"Producer started → AWS Kinesis ({REGION})")
    print(f"Stream: {STREAM_NAME}")
    print(f"Subreddits: {SUBREDDITS}\n")
    
    while True:
        total_new = 0
        for subreddit in SUBREDDITS:
            response = fetch_new_posts(subreddit)
            if not response:
                continue
            
            for post in response["data"]["children"]:
                post_data = extract_post_data(post)
                post_id = post_data["post_id"]
                
                if post_id in seen_post_ids:
                    continue
                
                seen_post_ids.add(post_id)
                total_new += 1
                
                seq = push_to_kinesis(post_data)
                if seq:
                    print(f"[{subreddit}] {post_data['title'][:60]}...")
        
        print(f"\n>>> {total_new} new posts pushed. Sleeping {POLL_INTERVAL}s...\n")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nStopped.")