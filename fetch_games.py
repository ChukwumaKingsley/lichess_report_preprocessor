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

# Upload JSON to Drive with enhanced retry logic and file splitting
def upload_to_drive(file_path, folder_id, mimetype="application/json", max_file_size_mb=15):
    file_name = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
    print(f"[{datetime.now()}] Attempting to upload '{file_name}' (size: {file_size_mb:.2f} MB)")

    if file_size_mb > max_file_size_mb:
        print(f"[{datetime.now()}] File size ({file_size_mb:.2f} MB) exceeds {max_file_size_mb} MB limit. Splitting file...")
        split_files = split_large_file(file_path, max_file_size_mb)
        for i, split_file in enumerate(split_files):
            split_name = f"{os.path.splitext(file_name)[0]}_part{i+1}.json"
            upload_single_file(os.path.join(PLAYER_FOLDER, split_name), folder_id, mimetype)
    else:
        upload_single_file(file_path, folder_id, mimetype)

def upload_single_file(file_path, folder_id, mimetype):
    file_name = os.path.basename(file_path)
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    max_retries = 5
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
        except (HttpError, IOError, ssl.SSLEOFError) as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"[{datetime.now()}] Retry {attempt + 1}/{max_retries} due to error: {e}. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"[{datetime.now()}] Failed after {max_retries} retries: {e}")
                raise

# Split large JSON file into smaller parts
def split_large_file(file_path, max_size_mb):
    max_size_bytes = max_size_mb * 1024 * 1024
    split_files = []
    current_size = 0
    current_lines = []
    part_num = 1

    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line_size = len(line.encode("utf-8"))
            if current_size + line_size > max_size_bytes and current_lines:
                split_file = f"{os.path.splitext(file_path)[0]}_part{part_num}.json"
                with open(split_file, "w", encoding="utf-8") as out_f:
                    out_f.write("".join(current_lines))
                split_files.append(split_file)
                current_lines = [line]
                current_size = line_size
                part_num += 1
            else:
                current_lines.append(line)
                current_size += line_size

    if current_lines:
        split_file = f"{os.path.splitext(file_path)[0]}_part{part_num}.json"
        with open(split_file, "w", encoding="utf-8") as out_f:
            out_f.write("".join(current_lines))
        split_files.append(split_file)

    return split_files

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

# Request new games
print(f"[{datetime.now()}] Fetching games for {USERNAME}...")
resp = requests.get(url, headers=headers, params=params, stream=True)
resp.raise_for_status()

# Append or create new local file
mode = "a" if os.path.exists(OUTPUT_JSON) else "w"
with open(OUTPUT_JSON, mode, encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode("utf-8")
        if decoded_line.strip():
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and saved to '{OUTPUT_JSON}' locally.")

# Upload updated JSON to the user's Drive folder
upload_to_drive(OUTPUT_JSON, USER_FOLDER_ID)
print(f"[{datetime.now()}] Uploaded '{OUTPUT_JSON}' to Drive folder '{USERNAME}'.")