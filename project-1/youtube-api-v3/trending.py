import os
import requests
import json
from dotenv import load_dotenv

# Load API key from environment variable
load_dotenv()
API_KEY = os.getenv("YOUTUBE_API_KEY")
MAX_RESULTS = 100

#Get Video IDs
trending_url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode=US&maxResults={MAX_RESULTS}&key={API_KEY}"

trending_response = requests.get(trending_url).json()

# Process data
trending_videos = []
for item in trending_response["items"]:
    trending_videos.append({
        "video_id": item["id"],
        "title": item["snippet"]["title"],
        "category": item["snippet"]["categoryId"],
        "views": int(item["statistics"].get("viewCount", 0)),
        "likes": int(item["statistics"].get("likeCount", 0)),
        "comments": int(item["statistics"].get("commentCount", 0)),
        "published_date": item["snippet"]["publishedAt"]
    })

print(json.dumps(trending_videos))