import os
import urllib.parse
import requests
from dotenv import load_dotenv

load_dotenv()

# Step 2 of OAuth setup: exchanges the one-time authorization code for a refresh token.
# After running youtube_token_connection.py and authorizing in the browser,
# paste the full redirected URL below (it contains ?code=...).

CLIENT_ID     = os.getenv("YOUTUBE_CLIENT_ID")
CLIENT_SECRET = os.getenv("YOUTUBE_CLIENT_SECRET")
REDIRECT_URI  = os.getenv("YOUTUBE_REDIRECT_URI", "https://www.kaviglobal.com")

# Paste the full URL you were redirected to after authorizing:
redirected_url = "PASTE_YOUR_REDIRECTED_URL_HERE"

parsed = urllib.parse.urlparse(redirected_url)
params = urllib.parse.parse_qs(parsed.query)

code = params["code"][0]
state_returned = params.get("state", [""])[0]

print("Code (first 15):", code[:15], "...")
print("Returned state:", state_returned)

resp = requests.post(
    "https://oauth2.googleapis.com/token",
    data={
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
    },
)

print("Status:", resp.status_code)
token = resp.json()
print(token)

# Copy the refresh_token value from above into your .env as YOUTUBE_REFRESH_TOKEN
