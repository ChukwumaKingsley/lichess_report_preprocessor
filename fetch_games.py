#!/usr/bin/env python3
"""
fetch_json.py

Fetches recent Lichess games for a user in JSON format,
writes them to a local file, and optionally parses to a DataFrame.
Supports incremental fetching based on the last retrieved game's timestamp.
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

# Check if previous file exists
start_time_param = None
if os.path.exists(OUTPUT_JSON):
    print(f"[{datetime.now()}] Previous JSON file found. Checking last game timestamp...")

    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if lines:
        # Parse the last line
        last_game = json.loads(lines[-1])
        if 'createdAt' in last_game:
            last_game_timestamp = int(last_game['createdAt']) // 1000  # convert ms to seconds
            start_time_param = last_game_timestamp + 1  # move forward slightly to avoid duplication
            params["since"] = start_time_param * 1000  # Lichess expects milliseconds
            print(f"[{datetime.now()}] Will fetch games since {datetime.utcfromtimestamp(start_time_param)} UTC.")
        else:
            print(f"[{datetime.now()}] Warning: No 'createdAt' field in last game. Fetching all games.")
    else:
        print(f"[{datetime.now()}] Warning: JSON file was empty. Fetching all games.")

else:
    print(f"[{datetime.now()}] No previous JSON file. Fetching all games.")

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
        if decoded_line.strip():  # only write non-empty lines
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and saved to '{OUTPUT_JSON}'.")
