#!/usr/bin/env python3
"""
fetch_games.py

Fetches recent Lichess games for a user in JSON format,
appends them to a local file, and uploads to a user-specific Google Drive folder.
Supports incremental fetching based on the most recent game's timestamp
from the Drive-stored JSON.

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
OUTPUT_JSON = os.path.join(PLAYER_FOLDER, f"games_{USERNAME}.json")

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

# Download existing JSON from Drive if available
def download_from_drive(file_name, folder_id):
    try:
        query = f"name='{os.path.basename(file_name)}' and '{folder_id}' in parents"
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
            with open(file_name, "wb") as f:
                f.write(fh.read())
            print(f"[{datetime.now()}] Downloaded existing '{os.path.basename(file_name)}' from Drive folder.")
            return True
    except Exception as e:
        print(f"[{datetime.now()}] Error downloading from Drive: {e}")
    return False

# Upload JSON to Drive with retry logic
def upload_to_drive(file_path, folder_id, mimetype="application/json"):
    file_name = os.path.basename(file_path)
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = drive_service.files().list(q=query, fields="files(id, name)").execute()
            files = response.get("files", [])
            media = MediaFileUpload(file_path, mimetype=mimetype)
            if files:
                file_id = files[0]["id"]
                drive_service.files().update(fileId=file_id, media_body=media).execute()
                print(f"[{datetime.now()}] Updated existing file '{file_name}' in Drive folder (ID: {file_id})")
            else:
                file_metadata = {"name": file_name, "parents": [folder_id]}
                new_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
                print(f"[{datetime.now()}] Uploaded new file '{file_name}' to Drive folder (ID: {new_file.get('id')})")
            break
        except (HttpError, IOError) as e:
            if attempt < max_retries - 1:
                print(f"[{datetime.now()}] Retry {attempt + 1}/{max_retries} due to error: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"[{datetime.now()}] Failed after {max_retries} retries: {e}")
                raise

# Fetch games in batches
def fetch_games_in_batches(url, headers, params, output_file):
    max_games_per_batch = 1000  # Adjust as needed
    mode = "a" if os.path.exists(output_file) else "w"
    total_games = 0
    since = params.get("since", None)
    until = int(datetime.now().timestamp() * 1000)  # Current timestamp in milliseconds

    while True:
        batch_params = params.copy()
        if since:
            batch_params["since"] = since
        batch_params["until"] = until
        batch_params["max"] = max_games_per_batch

        print(f"[{datetime.now()}] Fetching up to {max_games_per_batch} games from {datetime.utcfromtimestamp(since/1000 if since else 0)} UTC...")
        resp = requests.get(url, headers=headers, params=batch_params, stream=True)
        resp.raise_for_status()

        batch_games = 0
        last_created_at = since

        with open(output_file, mode, encoding="utf-8") as f:
            for line in resp.iter_lines():
                decoded_line = line.decode("utf-8")
                if decoded_line.strip():
                    game = json.loads(decoded_line)
                    if "createdAt" in game:
                        last_created_at = max(last_created_at if last_created_at else 0, game["createdAt"])
                    f.write(decoded_line + "\n")
                    batch_games += 1

        total_games += batch_games
        print(f"[{datetime.now()}] Fetched {batch_games} games in this batch (total: {total_games}).")

        if batch_games < max_games_per_batch:
            break  # No more games to fetch

        since = last_created_at + 1
        mode = "a"  # Append in subsequent batches

    return total_games

# Get the user's folder ID
USER_FOLDER_ID = get_or_create_user_folder(USERNAME)

# Download existing JSON from Drive to determine the most recent timestamp
max_created_at = 0
if download_from_drive(OUTPUT_JSON, USER_FOLDER_ID):
    print(f"[{datetime.now()}] Previous JSON file found. Finding most recent game timestamp...")
    with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                try:
                    game = json.loads(line)
                    if "createdAt" in game:
                        max_created_at = max(max_created_at, game["createdAt"])
                except json.JSONDecodeError:
                    print(f"[{datetime.now()}] Warning: Invalid JSON line: {line}")

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

# Fetch games in batches
total_games = fetch_games_in_batches(url, headers, params, OUTPUT_JSON)

# Log file size for debugging
file_size = os.path.getsize(OUTPUT_JSON) / (1024 * 1024)  # Size in MB
print(f"[{datetime.now()}] Games successfully fetched and saved to '{OUTPUT_JSON}' locally (file size: {file_size:.2f} MB, total games: {total_games}).")

# Upload updated JSON to the user's Drive folder
upload_to_drive(OUTPUT_JSON, USER_FOLDER_ID)
print(f"[{datetime.now()}] Uploaded '{OUTPUT_JSON}' to Drive folder '{USERNAME}'.")
