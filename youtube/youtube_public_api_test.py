import os
import requests
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("YOUTUBE_API_KEY")
HANDLE = "KaviGlobal"         # from https://www.youtube.com/@KaviGlobal
N_RECENT = 25                 # number of recent videos to pull

BASE = "https://www.googleapis.com/youtube/v3"

def yt_get(endpoint, params):
    params = dict(params)
    params["key"] = API_KEY
    r = requests.get(f"{BASE}/{endpoint}", params=params, timeout=30)
    # helpful for debugging quota/param issues
    if r.status_code != 200:
        raise RuntimeError(f"{endpoint} failed {r.status_code}: {r.text}")
    return r.json()

# --- 1) Resolve channel by handle (preferred) ---
chan = yt_get("channels", {
    "part": "id,snippet,statistics,contentDetails",
    "forHandle": HANDLE,
})

if not chan.get("items"):
    # Fallback: search by handle text if forHandle isn't available in your project/version
    srch = yt_get("search", {
        "part": "snippet",
        "q": f"@{HANDLE}",
        "type": "channel",
        "maxResults": 1,
    })
    if not srch.get("items"):
        raise RuntimeError("Could not resolve channel from handle.")
    channel_id = srch["items"][0]["snippet"]["channelId"]

    chan = yt_get("channels", {
        "part": "id,snippet,statistics,contentDetails",
        "id": channel_id,
    })

channel = chan["items"][0]
channel_id = channel["id"]

channel_row = {
    "pulled_at_utc": datetime.now(timezone.utc).isoformat(),
    "channel_id": channel_id,
    "channel_title": channel["snippet"].get("title"),
    "custom_url": channel["snippet"].get("customUrl"),
    "published_at": channel["snippet"].get("publishedAt"),
    "subscriber_count": int(channel["statistics"].get("subscriberCount", 0)),
    "view_count": int(channel["statistics"].get("viewCount", 0)),
    "video_count": int(channel["statistics"].get("videoCount", 0)),
}

print("CHANNEL:", channel_row)

# --- 2) Pull recent uploads via the uploads playlist ---
uploads_playlist = channel["contentDetails"]["relatedPlaylists"]["uploads"]

video_ids = []
page_token = None
while len(video_ids) < N_RECENT:
    pl = yt_get("playlistItems", {
        "part": "contentDetails",
        "playlistId": uploads_playlist,
        "maxResults": min(50, N_RECENT - len(video_ids)),
        "pageToken": page_token,
    })
    video_ids += [it["contentDetails"]["videoId"] for it in pl.get("items", [])]
    page_token = pl.get("nextPageToken")
    if not page_token:
        break

# --- 3) Fetch video details + statistics in batches of 50 ---
rows = []
for i in range(0, len(video_ids), 50):
    batch = video_ids[i:i+50]
    vids = yt_get("videos", {
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(batch),
        "maxResults": 50,
    })
    for v in vids.get("items", []):
        s = v.get("statistics", {})
        sn = v.get("snippet", {})
        rows.append({
            "channel_id": channel_id,
            "video_id": v["id"],
            "title": sn.get("title"),
            "published_at": sn.get("publishedAt"),
            "view_count": int(s.get("viewCount", 0)),
            "like_count": int(s.get("likeCount", 0)),
            "comment_count": int(s.get("commentCount", 0)),
            "duration": v.get("contentDetails", {}).get("duration"),
        })

df_videos = pd.DataFrame(rows).sort_values("published_at", ascending=False)

# --- 4) Save outputs for your meeting/demo ---
pd.DataFrame([channel_row]).to_csv("kaviglobal_channel_snapshot.csv", index=False)
df_videos.to_csv("kaviglobal_recent_videos.csv", index=False)

print("\nWrote: kaviglobal_channel_snapshot.csv")
print("Wrote: kaviglobal_recent_videos.csv")
print("\nTop 5 recent videos:\n", df_videos.head(5)[["published_at","title","view_count","like_count","comment_count"]])
