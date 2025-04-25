#!/usr/bin/env python3
"""
fetch_json.py

Fetches recent Lichess games for a user in JSON format,
writes them to a local file, and optionally parses to a DataFrame.
"""

import os
import requests
import json
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
TOKEN = os.getenv("LICHESS_TOKEN")
USERNAME = os.getenv("LICHESS_USERNAME")
OUTPUT_JSON = f"games_{USERNAME}_{datetime.now():%Y%m%d}.json"

# Check config
if not USERNAME:
    raise RuntimeError("LICHESS_USERNAME not set in .env")

# Setup request
headers = {
    "Accept": "application/x-ndjson"
}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

params = {
    "clocks": True,
    "opening": True,
    "evals": False,
    "pgnInJson": False,  # we’re not embedding PGN
}

url = f"https://lichess.org/api/games/user/{USERNAME}"

print(f"[{datetime.now()}] Fetching games for {USERNAME}...")

resp = requests.get(url, headers=headers, params=params, stream=True)
resp.raise_for_status()

# Decode each line from bytes to string
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode('utf-8')
        f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Saved JSON games to '{OUTPUT_JSON}'")

