#!/usr/bin/env python3
"""
fetch_json.py

Fetches recent Lichess games for a user in JSON format,
appends them to a local file. Supports incremental fetching
based on the most recent game's timestamp.
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
OUTPUT_JSON = f"games_{USERNAME}.json"

# Check config
if not USERNAME:
    raise RuntimeError("LICHESS_USERNAME not set in .env")

# Setup request headers
headers = {
    "Accept": "application/x-ndjson"
}
if TOKEN:
    headers["Authorization"] = f"Bearer {TOKEN}"

# Base request params
params = {
    "clocks": True,
    "opening": True,
    "evals": False,
    "pgnInJson": False,  # Not embedding PGN
}

# Find the most recent game's timestamp if the file exists
max_created_at = 0
if os.path.exists(OUTPUT_JSON):
    print(f"[{datetime.now()}] Previous JSON file found. Finding most recent game timestamp...")
    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    game = json.loads(line)
                    if 'createdAt' in game:
                        max_created_at = max(max_created_at, game['createdAt'])
                except json.JSONDecodeError:
                    print(f"[{datetime.now()}] Warning: Invalid JSON line: {line}")

if max_created_at > 0:
    params["since"] = max_created_at + 1  # Fetch games after the most recent
    print(f"[{datetime.now()}] Will fetch games since {datetime.utcfromtimestamp(max_created_at / 1000)} UTC.")
else:
    print(f"[{datetime.now()}] No previous games or unable to find timestamps. Fetching all games.")

# Prepare URL
url = f"https://lichess.org/api/games/user/{USERNAME}"

# Request new games
print(f"[{datetime.now()}] Fetching games for {USERNAME}...")
resp = requests.get(url, headers=headers, params=params, stream=True)
resp.raise_for_status()

# Append or create new file
mode = "a" if os.path.exists(OUTPUT_JSON) else "w"
with open(OUTPUT_JSON, mode, encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode('utf-8')
        if decoded_line.strip():  # Only write non-empty lines
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and saved to '{OUTPUT_JSON}'.")