#!/usr/bin/env python3
"""
fetch_rating_history.py

Fetches Lichess rating history for a user in JSON format,
saves it to a local file, and uploads to a user-specific Google Drive folder.

Usage: python fetch_rating_history.py <username>
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

# Load .env for LICHESS_TOKEN and DRIVE_PARENT_FOLDER_ID
load_dotenv()
LICHESS_TOKEN = os.getenv("LICHESS_TOKEN")
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

# Check config
if not LICHESS_TOKEN:
    raise RuntimeError("LICHESS_TOKEN not set in .env")
if not DRIVE_PARENT_FOLDER_ID:
    raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

# Build the Drive service (uses GOOGLE_APPLICATION_CREDENTIALS from workflow)
try:
    drive_service = build("drive", "v3")
except HttpError as e:
    raise RuntimeError(f"Failed to build Drive service: {e}")

# Get username from command-line argument
if len(sys.argv) != 2:
    raise RuntimeError("Usage: python fetch_rating_history.py <username>")
USERNAME = sys.argv[1]
OUTPUT_JSON = f"rating_history_{USERNAME}.json"

# Find or create a folder for the user
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
    file_name = os.path.basename(file_path)
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
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

# Get the user's folder ID
USER_FOLDER_ID = get_or_create_user_folder(USERNAME)

# Download existing JSON from Drive (optional, for consistency)
download_from_drive(OUTPUT_JSON, USER_FOLDER_ID)

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

# Upload updated JSON to the user's Drive folder
upload_to_drive(OUTPUT_JSON, USER_FOLDER_ID)
