#!/usr/bin/env python3
"""
fetch_json.py

Fetches recent Lichess games for a user in JSON format,
appends them to a local file, and uploads to a user-specific Google Drive folder.
Supports incremental fetching based on the most recent game's timestamp
from the Drive-stored JSON.

Usage: python fetch_json.py <username>
"""

import os
import sys
import requests
import json
from datetime import datetime
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io

# Load environment variables
load_dotenv()
TOKEN = os.getenv("LICHESS_TOKEN")
GOOGLE_DRIVE_CREDENTIALS = os.getenv("GOOGLE_DRIVE_CREDENTIALS")
PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

# Get username from command-line argument
if len(sys.argv) != 2:
    raise RuntimeError("Usage: python fetch_json.py <username>")
USERNAME = sys.argv[1]
OUTPUT_JSON = f"games_{USERNAME}.json"

# Check config
if not TOKEN:
    raise RuntimeError("LICHESS_TOKEN not set in .env")
if not GOOGLE_DRIVE_CREDENTIALS:
    raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS not set in .env")
if not PARENT_FOLDER_ID:
    raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

# Set up Google Drive credentials
creds = Credentials.from_service_account_info(json.loads(GOOGLE_DRIVE_CREDENTIALS))
drive_service = build("drive", "v3", credentials=creds)

# Find or create a folder for the user
def get_or_create_user_folder(username):
    query = f"name='{username}' and mimeType='application/vnd.google-apps.folder' and '{PARENT_FOLDER_ID}' in parents"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = response.get("files", [])

    if folders:
        folder_id = folders[0]["id"]
        print(f"[{datetime.now()}] Found existing folder for '{username}' with ID: {folder_id}")
    else:
        folder_metadata = {
            "name": username,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [PARENT_FOLDER_ID]
        }
        folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
        folder_id = folder.get("id")
        print(f"[{datetime.now()}] Created folder for '{username}' with ID: {folder_id}")
    return folder_id

# Download existing JSON from Drive if available
def download_from_drive(file_name, folder_id):
    try:
        query = f"name='{file_name}' and '{folder_id}' in parents"
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
            print(f"[{datetime.now()}] Downloaded existing '{file_name}' from Drive folder.")
            return True
    except Exception as e:
        print(f"[{datetime.now()}] Error downloading from Drive: {e}")
    return False

# Upload JSON to Drive
def upload_to_drive(file_path, folder_id, mimetype="application/json"):
    # Check if a file with the same name already exists in the folder
    file_name = os.path.basename(file_path)
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = response.get("files", [])

    media = MediaFileUpload(file_path, mimetype=mimetype)
    if files:
        # File exists, update it
        file_id = files[0]["id"]
        updated_file = drive_service.files().update(fileId=file_id, media_body=media).execute()
        print(f"[{datetime.now()}] Updated existing file '{file_name}' in Drive folder (ID: {file_id})")
    else:
        # File does not exist, create a new one
        file_metadata = {"name": file_name, "parents": [folder_id]}
        new_file = drive_service.files().create(body=file_metadata, media_body=media, fields="id").execute()
        print(f"[{datetime.now()}] Uploaded new file '{file_name}' to Drive folder (ID: {new_file.get('id')})")

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
headers = {"Accept": "application/x-ndjson", "Authorization": f"Bearer {TOKEN}"}

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
        if decoded_line.strip():  # Only write non-empty lines
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and saved to '{OUTPUT_JSON}' locally.")

# Upload updated JSON to the user's Drive folder
upload_to_drive(OUTPUT_JSON, USER_FOLDER_ID)
print(f"[{datetime.now()}] Uploaded '{OUTPUT_JSON}' to Drive folder '{USERNAME}'.")