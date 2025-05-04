#!/usr/bin/env python3
"""
preprocess_and_upload.py

Processes rating history data, saves it as a CSV file,
and uploads to a user-specific Google Drive folder.

Usage: python preprocess_and_upload.py <username>
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload
import time
import ssl  # Added for SSLEOFError handling

# Load .env for DRIVE_PARENT_FOLDER_ID
load_dotenv()
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID")

# Check config
if not DRIVE_PARENT_FOLDER_ID:
    raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

# Build the Drive service (uses GOOGLE_APPLICATION_CREDENTIALS from environment)
try:
    drive_service = build("drive", "v3")
except HttpError as e:
    raise RuntimeError(f"Failed to build Drive service: {e}")

# Get username from command-line argument
if len(sys.argv) != 2:
    raise RuntimeError("Usage: python preprocess_and_upload.py <username>")
USERNAME = sys.argv[1]

# Create player-specific folder under Player Data
PLAYER_DATA_FOLDER = os.path.join(os.getcwd(), "Player Data")
os.makedirs(PLAYER_DATA_FOLDER, exist_ok=True)
PLAYER_FOLDER = os.path.join(PLAYER_DATA_FOLDER, USERNAME)
os.makedirs(PLAYER_FOLDER, exist_ok=True)
RATING_HISTORY_INPUT_JSON = os.path.join(PLAYER_FOLDER, f"rating_history_{USERNAME}.json")
RATING_HISTORY_OUTPUT_CSV = os.path.join(PLAYER_FOLDER, f"rating_history_{USERNAME}.csv")

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

# Upload or update file to Drive with retry logic
def upload_to_drive(file_path, folder_id, mimetype="text/csv"):
    file_name = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
    print(f"[{datetime.now()}] Attempting to upload '{file_name}' (size: {file_size_mb:.2f} MB)")

    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = drive_service.files().list(q=f"name='{file_name}' and '{folder_id}' in parents and trashed=false", fields="files(id, name)").execute()
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

# Get the user's folder ID
USER_FOLDER_ID = get_or_create_user_folder(USERNAME)

# Check if input file exists locally (should be created by fetch_rating_history.py)
if not os.path.exists(RATING_HISTORY_INPUT_JSON):
    raise RuntimeError(f"[{datetime.now()}] Input file '{RATING_HISTORY_INPUT_JSON}' not found. Ensure fetch_rating_history.py ran first.")

# Process rating history
print(f"[{datetime.now()}] Processing rating history for {USERNAME}...")
rating_history = []
with open(RATING_HISTORY_INPUT_JSON, "r", encoding="utf-8") as f:
    rating_history = json.load(f)

rating_rows = []
for entry in rating_history:
    perfs = entry.get("points", [])
    category = entry.get("name")
    for point in perfs:
        try:
            # Correct month by adding 1
            date = datetime(point[0], point[1] + 1, point[2])
            rating_rows.append({"category": category, "date": date, "rating": point[3]})
        except ValueError:
            continue

rating_df = pd.DataFrame(rating_rows)
every = rating_df.pivot(index="date", columns="category", values="rating")
all_dates = pd.date_range(start=every.index.min(), end=every.index.max())
every = every.reindex(all_dates)
every_filled = every.ffill()
rating_final = every.combine_first(every_filled)
rating_final = rating_final.reset_index().rename(columns={"index": "date"})
if not pd.api.types.is_datetime64_any_dtype(rating_final['date']):
    rating_final['date'] = pd.to_datetime(rating_final['date'])
t_rating = rating_final.sort_values(by='date', ascending=False).reset_index(drop=True)
t_rating.to_csv(RATING_HISTORY_OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"[{datetime.now()}] Saved rating history to '{RATING_HISTORY_OUTPUT_CSV}'")

# Upload the CSV to Google Drive
upload_to_drive(RATING_HISTORY_OUTPUT_CSV, USER_FOLDER_ID, mimetype="text/csv")
print(f"[{datetime.now()}] Uploaded '{RATING_HISTORY_OUTPUT_CSV}' to Drive folder '{USERNAME}'.")