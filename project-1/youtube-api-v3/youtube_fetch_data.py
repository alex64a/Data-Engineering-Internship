import os
import requests
import json
from dotenv import load_dotenv

# Load API key from environment variable
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
QUERY = "bitcoin"
MAX_RESULTS = 5

#Get Video IDs
search_url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={QUERY}&type=video&maxResults={MAX_RESULTS}&key={API_KEY}"
search_response = requests.get(search_url).json()

video_ids = ",".join([item["id"]["videoId"] for item in search_response["items"]])

#Get Video Stats
video_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&id={video_ids}&key={API_KEY}"
video_response = requests.get(video_url).json()

#Extract relevant data
results = []
for item in video_response["items"]:
    results.append({
        "video_id": item["id"],
        "title": item["snippet"]["title"],
        "author": item["snippet"]["channelTitle"],
        "views": item["statistics"].get("viewCount", "N/A"),
        "likes": item["statistics"].get("likeCount", "N/A"),
        "comments": item["statistics"].get("commentCount", "N/A"),
    })

# Print JSON output (NiFi will read this)
print(json.dumps(results))