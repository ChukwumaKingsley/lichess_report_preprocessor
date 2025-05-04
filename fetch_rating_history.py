#!/usr/bin/env python3
"""
fetch_rating_history.py

Fetches Lichess rating history for a user in JSON format and saves it to a local file.

Usage: python fetch_rating_history.py <username>
"""

import os
import sys
import requests
from datetime import datetime
from dotenv import load_dotenv

# Load .env for LICHESS_TOKEN
load_dotenv()
LICHESS_TOKEN = os.getenv("LICHESS_TOKEN")

# Check config
if not LICHESS_TOKEN:
    raise RuntimeError("LICHESS_TOKEN not set in .env")

# Get username from command-line argument
if len(sys.argv) != 2:
    raise RuntimeError("Usage: python fetch_rating_history.py <username>")
USERNAME = sys.argv[1]

# Create player-specific folder under Player Data
PLAYER_DATA_FOLDER = os.path.join(os.getcwd(), "Player Data")
os.makedirs(PLAYER_DATA_FOLDER, exist_ok=True)
PLAYER_FOLDER = os.path.join(PLAYER_DATA_FOLDER, USERNAME)
os.makedirs(PLAYER_FOLDER, exist_ok=True)
OUTPUT_JSON = os.path.join(PLAYER_FOLDER, f"rating_history_{USERNAME}.json")

# Setup request
headers = {
    "Accept": "application/x-ndjson"
}
if LICHESS_TOKEN:
    headers["Authorization"] = f"Bearer {LICHESS_TOKEN}"

url = f"https://lichess.org/api/user/{USERNAME}/rating-history"

print(f"[{datetime.now()}] Fetching rating history for {USERNAME}...")
resp = requests.get(url, headers=headers, stream=True)
resp.raise_for_status()

# Decode each line from bytes to string and save locally
with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode('utf-8')
        if decoded_line.strip():  # Only write non-empty lines
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Saved rating history to '{OUTPUT_JSON}' locally.")