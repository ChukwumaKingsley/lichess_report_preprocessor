#!/usr/bin/env python3
"""
preprocess_and_upload.py

Processes new game data from a temporary JSON and appends it to an existing CSV,
processes rating history data, and uploads both to a user-specific Google Drive folder.

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
import ssl

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
TEMP_JSON = os.path.join(PLAYER_FOLDER, f"temp_games_{USERNAME}.json")
OUTPUT_CSV = os.path.join(PLAYER_FOLDER, f"games_{USERNAME}.csv")
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

# Helper functions for game preprocessing
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

# Process new games and combine with existing CSV
if os.path.exists(TEMP_JSON):
    print(f"[{datetime.now()}] Processing new games from '{TEMP_JSON}'...")
    new_games = []
    with open(TEMP_JSON, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                new_games.append(json.loads(line))

    if new_games:
        df_new = pd.DataFrame(new_games)
        df_new['played_as'], df_new['opponent_color'] = zip(*df_new.apply(get_sides, axis=1))
        df_new['player_name'] = USERNAME
        df_new['opponent_name'] = df_new.apply(lambda r: get_player_name(r['players'][r['opponent_color']]), axis=1)
        df_new['opponent_name'] = df_new.apply(lambda r: 'Lichess Stockfish' if r['source'] == 'ai' else r['opponent_name'], axis=1)
        df_new['player_rating'] = df_new.apply(lambda r: get_rating(r['players'][r['played_as']], 'rating'), axis=1)
        df_new['player_rating_diff'] = df_new.apply(lambda r: get_rating(r['players'][r['played_as']], 'ratingDiff'), axis=1)
        df_new['opponent_rating'] = df_new.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'rating'), axis=1)
        df_new['opponent_rating_diff'] = df_new.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'ratingDiff'), axis=1)
        df_new['result'] = df_new.apply(map_result, axis=1)
        df_new['opening_eco'] = df_new['opening'].apply(lambda o: o.get('eco') if isinstance(o, dict) else None)
        df_new['opening_name'] = df_new['opening'].apply(lambda o: o.get('name') if isinstance(o, dict) else None)
        df_new['opening_ply'] = df_new['opening'].apply(lambda o: o.get('ply') if isinstance(o, dict) else None)
        df_new['time_control'] = df_new['clock'].apply(format_time_control)
        df_new['game_id'] = df_new['id']
        df_new['rated'] = df_new['rated']
        df_new['speed'] = df_new['speed']
        df_new['created_at'] = pd.to_datetime(df_new['createdAt'], unit='ms')
        df_new['last_move_at'] = pd.to_datetime(df_new['lastMoveAt'], unit='ms')
        df_new['status'] = df_new['status']
        df_new['source'] = df_new['source']
        df_new['tournament'] = df_new.get('tournament') is not None
        df_new = df_new[df_new['variant'] == 'standard'].drop(columns=['variant'])
        if 'moves' in df_new.columns:
            df_new['move_count'] = df_new['moves'].apply(lambda m: len(m.split()) if isinstance(m, str) else None)
            df_new['turns'] = df_new['move_count'].apply(lambda mc: (mc + 1) // 2 if isinstance(mc, int) else None)
        else:
            df_new['move_count'] = None
            df_new['turns'] = None
        df_new.loc[df_new['opponent_name'].isnull() & (df_new['source'] == 'friend'), 'opponent_name'] = 'Unnamed'
        df_new.loc[df_new['time_control'].isnull() & (df_new['speed'] == 'correspondence'), 'time_control'] = 'daily'
        df_new['time_control'] = df_new['time_control'].apply(lambda x: x.replace("s", "m") if isinstance(x, str) and "+" in x else x)
        columns = ['game_id', 'rated', 'speed', 'created_at', 'last_move_at', 'status', 'source', 'player_name', 'played_as', 'opponent_name', 'opponent_color', 'player_rating', 'player_rating_diff', 'opponent_rating', 'opponent_rating_diff', 'result', 'opening_eco', 'opening_name', 'opening_ply', 'tournament', 'time_control', 'move_count', 'turns']
        df_new = df_new[columns]

        # Combine with existing CSV if it exists
        if os.path.exists(OUTPUT_CSV):
            df_old = pd.read_csv(OUTPUT_CSV, parse_dates=['created_at', 'last_move_at'])
            df_combined = pd.concat([df_new, df_old], ignore_index=True)
        else:
            df_combined = df_new
        
        # Ensure created_at is datetime before sorting
        df_combined['created_at'] = pd.to_datetime(df_combined['created_at'])
        df_combined = df_combined.sort_values(by='created_at', ascending=False).reset_index(drop=True)
        df_combined.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
        print(f"[{datetime.now()}] Combined and saved new games to '{OUTPUT_CSV}'.")
    else:
        print(f"[{datetime.now()}] No new games to process.")

    # Clean up temporary JSON
    if os.path.exists(TEMP_JSON):
        os.remove(TEMP_JSON)
        print(f"[{datetime.now()}] Removed temporary JSON file '{TEMP_JSON}'.")

# Process rating history
if os.path.exists(RATING_HISTORY_INPUT_JSON):
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

USER_FOLDER_ID = get_or_create_user_folder(USERNAME)
# Upload both CSVs to Google Drive
if os.path.exists(OUTPUT_CSV):
    upload_to_drive(OUTPUT_CSV, USER_FOLDER_ID, mimetype="text/csv")
if os.path.exists(RATING_HISTORY_OUTPUT_CSV):
    upload_to_drive(RATING_HISTORY_OUTPUT_CSV, USER_FOLDER_ID, mimetype="text/csv")