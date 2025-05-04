#!/usr/bin/env python3
"""
fetch_games.py

Fetches recent Lichess games for a user in JSON format,
processes them temporarily, and uploads only the preprocessed CSV to Google Drive.
Supports incremental fetching based on the most recent game's timestamp.

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
import ssl  # Added to fix NameError

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
OUTPUT_CSV = os.path.join(PLAYER_FOLDER, f"games_{USERNAME}.csv")
TEMP_JSON = os.path.join(PLAYER_FOLDER, f"temp_games_{USERNAME}.json")  # Temporary JSON file

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

# Download existing CSV from Drive if available (for incremental updates)
def download_from_drive(file_path, folder_id, mimetype="text/csv"):
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

# Upload CSV to Drive with enhanced retry logic
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

# Download existing CSV to determine the most recent timestamp (for incremental updates)
max_created_at = 0
if download_from_drive(OUTPUT_CSV, USER_FOLDER_ID, mimetype="text/csv"):
    print(f"[{datetime.now()}] Previous CSV file found. Finding most recent game timestamp...")
    import pandas as pd
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

# Fetch games and process in-memory
print(f"[{datetime.now()}] Fetching games for {USERNAME}...")
resp = requests.get(url, headers=headers, params=params, stream=True)
resp.raise_for_status()

games = []
with open(TEMP_JSON, "w", encoding="utf-8") as f:
    for line in resp.iter_lines():
        decoded_line = line.decode("utf-8")
        if decoded_line.strip():
            game = json.loads(decoded_line)
            games.append(game)
            f.write(decoded_line + "\n")

print(f"[{datetime.now()}] Games successfully fetched and temporarily saved to '{TEMP_JSON}'.")

# Preprocess games in-memory and save as CSV
import pandas as pd

def get_player_name(player_dict):
    return player_dict.get('user', {}).get('name') or player_dict.get('name')

def get_sides(row):
    w = get_player_name(row['players']['white'])
    b = get_player_name(row['players']['black'])
    if w and w.lower() == USERNAME.lower():
        return 'white', 'black'
    if b and b.lower() == USERNAME.lower():
        return 'black', 'white'
    return 'white', 'black'

def get_rating(player_dict, field):
    return player_dict.get(field)

def map_result(row):
    winner = row.get('winner')
    if not isinstance(winner, str):
        return 'draw'
    return 'win' if winner.lower() == row['played_as'] else 'lose'

def format_time_control(clock):
    if isinstance(clock, dict):
        initial = clock.get('initial')
        increment = clock.get('increment')
        if initial is not None and increment is not None:
            if initial < 60:
                from math import gcd
                common_divisor = gcd(initial, 60)
                numerator = initial // common_divisor
                denominator = 60 // common_divisor
                return f"{numerator}/{denominator}+{increment}"
            else:
                initial_minutes = initial // 60
                return f"{initial_minutes}+{increment}"
    return None

df = pd.DataFrame(games)
df['played_as'], df['opponent_color'] = zip(*df.apply(get_sides, axis=1))
df['player_name'] = USERNAME
df['opponent_name'] = df.apply(lambda r: get_player_name(r['players'][r['opponent_color']]), axis=1)
df['opponent_name'] = df.apply(lambda r: 'Lichess Stockfish' if r['source'] == 'ai' else r['opponent_name'], axis=1)
df['player_rating'] = df.apply(lambda r: get_rating(r['players'][r['played_as']], 'rating'), axis=1)
df['player_rating_diff'] = df.apply(lambda r: get_rating(r['players'][r['played_as']], 'ratingDiff'), axis=1)
df['opponent_rating'] = df.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'rating'), axis=1)
df['opponent_rating_diff'] = df.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'ratingDiff'), axis=1)
df['result'] = df.apply(map_result, axis=1)
df['opening_eco'] = df['opening'].apply(lambda o: o.get('eco') if isinstance(o, dict) else None)
df['opening_name'] = df['opening'].apply(lambda o: o.get('name') if isinstance(o, dict) else None)
df['opening_ply'] = df['opening'].apply(lambda o: o.get('ply') if isinstance(o, dict) else None)
df['time_control'] = df['clock'].apply(format_time_control)
df['game_id'] = df['id']
df['rated'] = df['rated']
df['speed'] = df['speed']
df['created_at'] = pd.to_datetime(df['createdAt'], unit='ms')
df['last_move_at'] = pd.to_datetime(df['lastMoveAt'], unit='ms')
df['status'] = df['status']
df['source'] = df['source']
df['tournament'] = df.get('tournament') is not None
df = df[df['variant'] == 'standard'].drop(columns=['variant'])
if 'moves' in df.columns:
    df['move_count'] = df['moves'].apply(lambda m: len(m.split()) if isinstance(m, str) else None)
    df['turns'] = df['move_count'].apply(lambda mc: (mc + 1) // 2 if isinstance(mc, int) else None)
else:
    df['move_count'] = None
    df['turns'] = None
df.loc[df['opponent_name'].isnull() & (df['source'] == 'friend'), 'opponent_name'] = 'Unnamed'
df.loc[df['time_control'].isnull() & (df['speed'] == 'correspondence'), 'time_control'] = 'daily'
df['time_control'] = df['time_control'].apply(lambda x: x.replace("s", "m") if isinstance(x, str) and "+" in x else x)
columns = ['game_id', 'rated', 'speed', 'created_at', 'last_move_at', 'status', 'source', 'player_name', 'played_as', 'opponent_name', 'opponent_color', 'player_rating', 'player_rating_diff', 'opponent_rating', 'opponent_rating_diff', 'result', 'opening_eco', 'opening_name', 'opening_ply', 'tournament', 'time_control', 'move_count', 'turns']
final_df = df[columns]
final_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"[{datetime.now()}] Preprocessed games saved to '{OUTPUT_CSV}'.")

# Clean up temporary JSON file
if os.path.exists(TEMP_JSON):
    os.remove(TEMP_JSON)
    print(f"[{datetime.now()}] Removed temporary JSON file '{TEMP_JSON}'.")

# Upload the CSV to Google Drive
upload_to_drive(OUTPUT_CSV, USER_FOLDER_ID, mimetype="text/csv")
print(f"[{datetime.now()}] Uploaded '{OUTPUT_CSV}' to Drive folder '{USERNAME}'.")