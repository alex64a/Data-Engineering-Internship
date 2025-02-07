import tweepy
import json
import config
from datetime import datetime

# Authenticate with Twitter API v2
def authenticate():
    return tweepy.Client(bearer_token=config.BEARER_TOKEN)

# Function to fetch tweets using Twitter API v2
def get_tweets(client, query="Trending Netflix shows", max_results=10):
    response = client.search_recent_tweets(query=query, tweet_fields=["created_at", "public_metrics", "text"], max_results=max_results)
    tweet_data = []
    if response.data:
        for tweet in response.data:
            tweet_info = {
                "created_at": tweet.created_at.strftime("%Y-%m-%d %H:%M:%S"),
                "text": tweet.text,
                "likes": tweet.public_metrics["like_count"],
                "retweets": tweet.public_metrics["retweet_count"]
            }
            tweet_data.append(tweet_info)
    return tweet_data

# Function to save tweets data to a JSON file
def save_to_json(data, filename="tweets.json"):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)
        print(f"Data saved to {filename}")

# Main Function
def main():
    client = authenticate()
    tweets = get_tweets(client)
    if tweets:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"tweets_{timestamp}.json"
        save_to_json(tweets, filename)

if __name__ == "__main__":
    main()
