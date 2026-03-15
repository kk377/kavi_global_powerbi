import os
import secrets
import urllib.parse
import webbrowser
from dotenv import load_dotenv

load_dotenv()

# Step 1 of OAuth setup: generates the Google authorization URL and opens it in the browser.
# After authorizing, Google redirects to REDIRECT_URI with a ?code=... param.
# Copy that full redirected URL and paste it into youtube_token_collection.py.

CLIENT_ID    = os.getenv("YOUTUBE_CLIENT_ID")
REDIRECT_URI = os.getenv("YOUTUBE_REDIRECT_URI", "https://www.kaviglobal.com")

SCOPES = [
    "https://www.googleapis.com/auth/youtube.readonly",
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/yt-analytics-monetary.readonly",
]

STATE = secrets.token_urlsafe(16)

params = {
    "client_id": CLIENT_ID,
    "redirect_uri": REDIRECT_URI,
    "response_type": "code",
    "scope": " ".join(SCOPES),
    "access_type": "offline",
    "prompt": "consent",
    "include_granted_scopes": "true",
    "state": STATE,
}

auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode(params)
print("Open this URL:\n", auth_url)
print("\nSTATE to expect back:", STATE)

webbrowser.open(auth_url)
