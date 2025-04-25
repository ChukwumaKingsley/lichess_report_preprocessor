import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
TOKEN = os.getenv("LICHESS_TOKEN")
USERNAME = os.getenv("LICHESS_USERNAME")
OUTPUT_JSON = f"rating_history_{USERNAME}.json"

# Check config
if not USERNAME:
    raise RuntimeError("LICHESS_USERNAME not set in .env")

# Setup request
headers = {
    "Accept": "application/x-ndjson"
}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

url = f"https://lichess.org/api/user/{USERNAME}/rating-history"

print(f"[{datetime.now()}] Fetching rating history for {USERNAME}...")

resp = requests.get(url, headers=headers, stream=True)
resp.raise_for_status()

# Decode each line from bytes to string
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode('utf-8')
        f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Saved JSON games to '{OUTPUT_JSON}'")