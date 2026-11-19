#!/usr/bin/env python3
"""
fetch_games.py

Fetches new Lichess games for a user based on the latest timestamp from an existing CSV,
saving them to a temporary JSON file for further processing.

Usage: python fetch_games.py <username>
"""

import io
import os
import re
import sys
from datetime import datetime, timezone

import pandas as pd
import requests
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

USERNAME_RE = re.compile(r"^[A-Za-z0-9_-]{2,30}$")


def validate_username(username: str) -> str:
    cleaned = (username or "").strip()
    if not USERNAME_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid Lichess username: '{username}'")
    return cleaned


def escape_drive_query(val: str) -> str:
    return val.replace("\\", "\\\\").replace("'", "\\'")


def get_or_create_user_folder(drive_service, username: str, parent_folder_id: str) -> str:
    safe_user = escape_drive_query(username)
    safe_parent = escape_drive_query(parent_folder_id)
    query = (
        f"name='{safe_user}' and mimeType='application/vnd.google-apps.folder' "
        f"and '{safe_parent}' in parents and trashed = false"
    )
    response = drive_service.files().list(q=query, fields="files(id, name)").execute()
    folders = response.get("files", [])

    if folders:
        folder_id = folders[0]["id"]
        print(
            f"[{datetime.now(timezone.utc)}] Found existing folder for '{username}' with ID: {folder_id}"
        )
    else:
        folder_metadata = {
            "name": username,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id],
        }
        folder = (
            drive_service.files().create(body=folder_metadata, fields="id").execute()
        )
        folder_id = folder.get("id")
        print(
            f"[{datetime.now(timezone.utc)}] Created folder for '{username}' with ID: {folder_id}"
        )
    return folder_id


def download_from_drive(drive_service, file_path: str, folder_id: str) -> bool:
    try:
        safe_name = escape_drive_query(os.path.basename(file_path))
        safe_folder = escape_drive_query(folder_id)
        query = (
            f"name='{safe_name}' and '{safe_folder}' in parents and trashed = false"
        )
        files = drive_service.files().list(q=query, fields="files(id, name)").execute()
        if files.get("files"):
            file_id = files["files"][0]["id"]
            request = drive_service.files().get_media(fileId=file_id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    print(
                        f"[{datetime.now(timezone.utc)}] Downloaded {int(status.progress() * 100)}%."
                    )
            fh.seek(0)
            with open(file_path, "wb") as f:
                f.write(fh.read())
            print(
                f"[{datetime.now(timezone.utc)}] Downloaded existing '{os.path.basename(file_path)}' from Drive folder."
            )
            return True
    except Exception as e:
        print(f"[{datetime.now(timezone.utc)}] Error downloading from Drive: {e}")
    return False


def compute_max_created_at_ms(csv_path: str) -> int:
    if not os.path.exists(csv_path):
        return 0
    try:
        df = pd.read_csv(csv_path, usecols=lambda c: c == "created_at")
        if "created_at" not in df.columns or df.empty:
            return 0
        parsed = pd.to_datetime(df["created_at"], errors="coerce", utc=True).dropna()
        if parsed.empty:
            return 0
        max_dt = parsed.max()
        if pd.isna(max_dt):
            return 0
        return max(0, int(max_dt.timestamp() * 1000))
    except Exception as exc:
        print(f"[{datetime.now(timezone.utc)}] Warning reading timestamps from CSV: {exc}")
        return 0


def main() -> None:
    load_dotenv()
    lichess_token = os.getenv("LICHESS_TOKEN")
    drive_parent_folder_id = os.getenv("DRIVE_PARENT_FOLDER_ID")

    if not lichess_token:
        raise RuntimeError("LICHESS_TOKEN not set in .env")
    if not drive_parent_folder_id:
        raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python fetch_games.py <username>")
    username = validate_username(sys.argv[1])

    try:
        drive_service = build("drive", "v3", cache_discovery=False)
    except HttpError as e:
        raise RuntimeError(f"Failed to build Drive service: {e}") from e

    player_data_folder = os.path.join(os.getcwd(), "Player Data")
    player_folder = os.path.join(player_data_folder, username)
    os.makedirs(player_folder, exist_ok=True)
    temp_json = os.path.join(player_folder, f"temp_games_{username}.json")
    output_csv = os.path.join(player_folder, f"games_{username}.csv")

    user_folder_id = get_or_create_user_folder(
        drive_service, username, drive_parent_folder_id
    )

    max_created_at = 0
    if download_from_drive(drive_service, output_csv, user_folder_id):
        max_created_at = compute_max_created_at_ms(output_csv)

    if max_created_at > 0:
        params = {
            "since": max_created_at + 1,
            "clocks": True,
            "opening": True,
            "evals": False,
            "pgnInJson": False,
        }
        since_dt = datetime.fromtimestamp(max_created_at / 1000, tz=timezone.utc)
        print(f"[{datetime.now(timezone.utc)}] Will fetch games since {since_dt.isoformat()}.")
    else:
        params = {
            "clocks": True,
            "opening": True,
            "evals": False,
            "pgnInJson": False,
        }
        print(
            f"[{datetime.now(timezone.utc)}] No previous games or timestamps found. Fetching all games."
        )

    headers = {
        "Accept": "application/x-ndjson",
        "Authorization": f"Bearer {lichess_token}",
    }
    url = f"https://lichess.org/api/games/user/{username}"

    print(f"[{datetime.now(timezone.utc)}] Fetching games for {username}...")
    resp = requests.get(url, headers=headers, params=params, stream=True, timeout=60)
    resp.raise_for_status()

    with open(temp_json, "w", encoding="utf-8") as f:
        for line in resp.iter_lines():
            if not line:
                continue
            decoded_line = line.decode("utf-8").strip()
            if decoded_line:
                f.write(decoded_line + "\n")

    print(
        f"[{datetime.now(timezone.utc)}] Games successfully fetched and saved to '{temp_json}'."
    )


if __name__ == "__main__":
    main()