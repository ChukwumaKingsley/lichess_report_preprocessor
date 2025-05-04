#!/usr/bin/env python3
"""
fetch_games.py

Fetches new Lichess games for a user based on the latest timestamp from an existing CSV,
saving them to a temporary JSON file for further processing.

Usage: python fetch_games.py <username>
"""

import os
import sys
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
import time
import ssl

# Load .env for LICHESS_TOKEN and DRIVE_PARENT_FOLDER_ID
load_dotenv()
LICHESS_TOKEN = os.getenv("LICHESS_TOKEN")
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

# Check config
if not LICHESS_TOKEN:
    raise RuntimeError("LICHESS_TOKEN not set in .env")
if not DRIVE_PARENT_FOLDER_ID:
    raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

# Build the Drive service (uses GOOGLE_APPLICATION_CREDENTIALS from environment)
try:
    drive_service = build("drive", "v3")
except HttpError as e:
    raise RuntimeError(f"Failed to build Drive service: {e}")

# Get username from command-line argument
if len(sys.argv) != 2:
    raise RuntimeError("Usage: python fetch_games.py <username>")
USERNAME = sys.argv[1]

# Create player-specific folder under Player Data
PLAYER_DATA_FOLDER = os.path.join(os.getcwd(), "Player Data")
os.makedirs(PLAYER_DATA_FOLDER, exist_ok=True)
PLAYER_FOLDER = os.path.join(PLAYER_DATA_FOLDER, USERNAME)
os.makedirs(PLAYER_FOLDER, exist_ok=True)
TEMP_JSON = os.path.join(PLAYER_FOLDER, f"temp_games_{USERNAME}.json")
OUTPUT_CSV = os.path.join(PLAYER_FOLDER, f"games_{USERNAME}.csv")

# Find or create a folder for the user on Google Drive
def get_or_create_user_folder(username):
    query = f"name='{username}' and mimeType='application/vnd.google-apps.folder' and '{DRIVE_PARENT_FOLDER_ID}' in parents"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = response.get("files", [])

    if folders:
        folder_id = folders[0]["id"]
        print(f"[{datetime.now()}] Found existing folder for '{username}' with ID: {folder_id}")
    else:
        folder_metadata = {
            "name": username,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [DRIVE_PARENT_FOLDER_ID]
        }
        folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
        folder_id = folder.get("id")
        print(f"[{datetime.now()}] Created folder for '{username}' with ID: {folder_id}")
    return folder_id

# Download existing CSV from Drive if available
def download_from_drive(file_path, folder_id):
    try:
        query = f"name='{os.path.basename(file_path)}' and '{folder_id}' in parents"
        files = drive_service.files().list(q=query, fields="files(id, name)").execute()
        if files["files"]:
            file_id = files["files"][0]["id"]
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                print(f"[{datetime.now()}] Downloaded {int(status.progress() * 100)}%.")
            fh.seek(0)
            with open(file_path, "wb") as f:
                f.write(fh.read())
            print(f"[{datetime.now()}] Downloaded existing '{os.path.basename(file_path)}' from Drive folder.")
            return True
    except Exception as e:
        print(f"[{datetime.now()}] Error downloading from Drive: {e}")
    return False

# Get the user's folder ID
USER_FOLDER_ID = get_or_create_user_folder(USERNAME)

# Determine the latest timestamp from existing CSV
max_created_at = 0
if download_from_drive(OUTPUT_CSV, USER_FOLDER_ID):
    import pandas as pd
    if os.path.exists(OUTPUT_CSV):
        df = pd.read_csv(OUTPUT_CSV)
        if 'created_at' in df.columns:
            max_created_at = int(pd.to_datetime(df['created_at']).max().timestamp() * 1000)

if max_created_at > 0:
    params = {"since": max_created_at + 1, "clocks": True, "opening": True, "evals": False, "pgnInJson": False}
    print(f"[{datetime.now()}] Will fetch games since {datetime.utcfromtimestamp(max_created_at / 1000)} UTC.")
else:
    params = {"clocks": True, "opening": True, "evals": False, "pgnInJson": False}
    print(f"[{datetime.now()}] No previous games or unable to find timestamps. Fetching all games.")

# Setup request headers
headers = {"Accept": "application/x-ndjson", "Authorization": f"Bearer {LICHESS_TOKEN}"}

# Prepare URL
url = f"https://lichess.org/api/games/user/{USERNAME}"

# Fetch new games and save to temporary JSON
print(f"[{datetime.now()}] Fetching games for {USERNAME}...")
resp = requests.get(url, headers=headers, params=params, stream=True)
resp.raise_for_status()

with open(TEMP_JSON, "w", encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode("utf-8")
        if decoded_line.strip():
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and saved to '{TEMP_JSON}'.")