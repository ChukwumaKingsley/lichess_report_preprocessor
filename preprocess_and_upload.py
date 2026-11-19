#!/usr/bin/env python3
"""
preprocess_and_upload.py

Processes new game data from a temporary JSON and appends it to an existing CSV,
processes rating history data, and uploads both to a user-specific Google Drive folder.

Usage: python preprocess_and_upload.py <username>
"""

import json
import os
import re
import ssl
import sys
import time
from datetime import datetime, timezone
from math import gcd
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

USERNAME_RE = re.compile(r"^[A-Za-z0-9_-]{2,30}$")

GAME_COLUMNS = [
    "game_id",
    "rated",
    "speed",
    "created_at",
    "last_move_at",
    "status",
    "source",
    "player_name",
    "played_as",
    "opponent_name",
    "opponent_color",
    "player_rating",
    "player_rating_diff",
    "opponent_rating",
    "opponent_rating_diff",
    "result",
    "opening_eco",
    "opening_name",
    "opening_ply",
    "tournament",
    "time_control",
    "move_count",
    "turns",
]


def validate_username(username: str) -> str:
    cleaned = (username or "").strip()
    if not USERNAME_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid Lichess username: '{username}'")
    return cleaned


def escape_drive_query(val: str) -> str:
    return val.replace("\\", "\\\\").replace("'", "\\'")


def get_or_create_user_folder(
    drive_service, username: str, parent_folder_id: str
) -> str:
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


def upload_to_drive(
    drive_service, file_path: str, folder_id: str, mimetype: str = "text/csv"
) -> None:
    file_name = os.path.basename(file_path)
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(
        f"[{datetime.now(timezone.utc)}] Attempting to upload '{file_name}' (size: {file_size_mb:.2f} MB)"
    )

    safe_name = escape_drive_query(file_name)
    safe_folder = escape_drive_query(folder_id)
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = (
                drive_service.files()
                .list(
                    q=f"name='{safe_name}' and '{safe_folder}' in parents and trashed = false",
                    fields="files(id, name)",
                )
                .execute()
            )
            files = response.get("files", [])
            media = MediaFileUpload(file_path, mimetype=mimetype)
            if files:
                file_id = files[0]["id"]
                drive_service.files().update(
                    fileId=file_id, media_body=media
                ).execute()
                print(
                    f"[{datetime.now(timezone.utc)}] Updated existing file '{file_name}' in Drive folder (ID: {file_id})"
                )
            else:
                file_metadata = {"name": file_name, "parents": [folder_id]}
                new_file = (
                    drive_service.files()
                    .create(body=file_metadata, media_body=media, fields="id")
                    .execute()
                )
                print(
                    f"[{datetime.now(timezone.utc)}] Uploaded new file '{file_name}' to Drive folder (ID: {new_file.get('id')})"
                )
            break
        except (HttpError, IOError, ssl.SSLEOFError) as e:
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                print(
                    f"[{datetime.now(timezone.utc)}] Retry {attempt + 1}/{max_retries} due to error: {e}. Waiting {wait_time} seconds..."
                )
                time.sleep(wait_time)
            else:
                print(
                    f"[{datetime.now(timezone.utc)}] Failed after {max_retries} retries: {e}"
                )
                raise


def get_player_name(player_dict: Optional[Dict[str, Any]]) -> Optional[str]:
    if not isinstance(player_dict, dict):
        return None
    user_obj = player_dict.get("user")
    if isinstance(user_obj, dict) and user_obj.get("name"):
        return str(user_obj["name"])
    if player_dict.get("name"):
        return str(player_dict["name"])
    return None


def resolve_sides(
    players_dict: Optional[Dict[str, Any]], username: str
) -> Tuple[str, str]:
    if not isinstance(players_dict, dict):
        return "white", "black"
    w = get_player_name(players_dict.get("white"))
    b = get_player_name(players_dict.get("black"))
    uname_lower = username.lower()
    if w and w.lower() == uname_lower:
        return "white", "black"
    if b and b.lower() == uname_lower:
        return "black", "white"
    return "white", "black"


def format_time_control(clock: Any) -> Optional[str]:
    if not isinstance(clock, dict):
        return None
    initial = clock.get("initial")
    increment = clock.get("increment")
    if initial is None or increment is None:
        return None
    try:
        initial_int = int(initial)
        increment_int = int(increment)
    except (TypeError, ValueError):
        return None

    if initial_int == 0:
        return f"0+{increment_int}"
    if initial_int % 60 == 0:
        return f"{initial_int // 60}+{increment_int}"
    common_divisor = gcd(initial_int, 60)
    numerator = initial_int // common_divisor
    denominator = 60 // common_divisor
    return f"{numerator}/{denominator}+{increment_int}"


def transform_games_records(
    raw_games: List[Dict[str, Any]], username: str
) -> pd.DataFrame:
    """
    Single-pass transformation of raw Lichess game JSON records into a normalized DataFrame.
    Fixes BUG-01 (tournament boolean), BUG-02 (missing keys / non-standard variants),
    BUG-07 (90+0 time controls), and PERF-04 (eliminates 11 row-wise DataFrame.apply calls).
    """
    rows: List[Dict[str, Any]] = []
    for g in raw_games:
        if not isinstance(g, dict):
            continue
        if g.get("variant", "standard") != "standard":
            continue

        players = g.get("players") if isinstance(g.get("players"), dict) else {}
        played_as, opp_color = resolve_sides(players, username)
        p_side = players.get(played_as) if isinstance(players.get(played_as), dict) else {}
        o_side = players.get(opp_color) if isinstance(players.get(opp_color), dict) else {}

        source = g.get("source")
        speed = g.get("speed")
        opp_name = get_player_name(o_side)
        if source == "ai":
            opp_name = "Lichess Stockfish"
        elif opp_name is None and source == "friend":
            opp_name = "Unnamed"

        winner = g.get("winner")
        if not isinstance(winner, str):
            result = "draw"
        else:
            result = "win" if winner.lower() == played_as else "lose"

        opening = g.get("opening") if isinstance(g.get("opening"), dict) else {}
        tc = format_time_control(g.get("clock"))
        if tc is None and speed == "correspondence":
            tc = "daily"

        moves_str = g.get("moves")
        if isinstance(moves_str, str):
            tokens = moves_str.split()
            move_count: Optional[int] = len(tokens)
            turns: Optional[int] = (move_count + 1) // 2
        else:
            move_count = None
            turns = None

        created_ms = g.get("createdAt")
        last_move_ms = g.get("lastMoveAt")
        created_at = (
            pd.to_datetime(created_ms, unit="ms") if created_ms is not None else pd.NaT
        )
        last_move_at = (
            pd.to_datetime(last_move_ms, unit="ms")
            if last_move_ms is not None
            else pd.NaT
        )

        # BUG-01 fix: check per-row tournament presence/truthiness
        tournament_val = g.get("tournament")
        is_tournament = bool(
            tournament_val is not None
            and not pd.isna(tournament_val)
            and str(tournament_val).strip() != ""
        )

        rows.append(
            {
                "game_id": g.get("id"),
                "rated": bool(g.get("rated", False)),
                "speed": speed,
                "created_at": created_at,
                "last_move_at": last_move_at,
                "status": g.get("status"),
                "source": source,
                "player_name": username,
                "played_as": played_as,
                "opponent_name": opp_name,
                "opponent_color": opp_color,
                "player_rating": p_side.get("rating"),
                "player_rating_diff": p_side.get("ratingDiff"),
                "opponent_rating": o_side.get("rating"),
                "opponent_rating_diff": o_side.get("ratingDiff"),
                "result": result,
                "opening_eco": opening.get("eco"),
                "opening_name": opening.get("name"),
                "opening_ply": opening.get("ply"),
                "tournament": is_tournament,
                "time_control": tc,
                "move_count": move_count,
                "turns": turns,
            }
        )

    if not rows:
        return pd.DataFrame(columns=GAME_COLUMNS)
    return pd.DataFrame(rows, columns=GAME_COLUMNS)


def transform_rating_history(rating_history: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Transform Lichess rating history JSON array into a daily forward-filled DataFrame.
    Safely handles empty rating histories and multiple points on the same date (BUG-02).
    """
    rating_rows: List[Dict[str, Any]] = []
    for entry in rating_history or []:
        if not isinstance(entry, dict):
            continue
        perfs = entry.get("points", [])
        category = entry.get("name")
        if not category or not isinstance(perfs, list):
            continue
        for point in perfs:
            if not isinstance(point, (list, tuple)) or len(point) < 4:
                continue
            try:
                date = datetime(int(point[0]), int(point[1]) + 1, int(point[2]))
                rating_rows.append(
                    {"category": str(category), "date": date, "rating": point[3]}
                )
            except (ValueError, TypeError):
                continue

    if not rating_rows:
        return pd.DataFrame(columns=["date"])

    rating_df = pd.DataFrame(rating_rows)
    # Use pivot_table with aggfunc="last" to avoid crashing on duplicate (date, category) pairs
    every = rating_df.pivot_table(
        index="date", columns="category", values="rating", aggfunc="last"
    )
    if every.empty:
        return pd.DataFrame(columns=["date"])

    all_dates = pd.date_range(start=every.index.min(), end=every.index.max())
    every = every.reindex(all_dates)
    every_filled = every.ffill()
    rating_final = every.combine_first(every_filled)
    rating_final = rating_final.reset_index().rename(columns={"index": "date"})
    if not pd.api.types.is_datetime64_any_dtype(rating_final["date"]):
        rating_final["date"] = pd.to_datetime(rating_final["date"])
    return rating_final.sort_values(by="date", ascending=False).reset_index(drop=True)


def main() -> None:
    load_dotenv()
    drive_parent_folder_id = os.getenv("DRIVE_PARENT_FOLDER_ID")
    if not drive_parent_folder_id:
        raise RuntimeError("DRIVE_PARENT_FOLDER_ID not set in .env")

    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python preprocess_and_upload.py <username>")
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
    rating_history_input_json = os.path.join(
        player_folder, f"rating_history_{username}.json"
    )
    rating_history_output_csv = os.path.join(
        player_folder, f"rating_history_{username}.csv"
    )

    if os.path.exists(temp_json):
        print(
            f"[{datetime.now(timezone.utc)}] Processing new games from '{temp_json}'..."
        )
        new_games: List[Dict[str, Any]] = []
        with open(temp_json, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    new_games.append(json.loads(line))

        df_new = transform_games_records(new_games, username)
        if not df_new.empty:
            if os.path.exists(output_csv):
                df_old = pd.read_csv(
                    output_csv, parse_dates=["created_at", "last_move_at"]
                )
                df_combined = pd.concat([df_new, df_old], ignore_index=True)
            else:
                df_combined = df_new

            df_combined["created_at"] = pd.to_datetime(
                df_combined["created_at"], errors="coerce"
            )
            if "game_id" in df_combined.columns:
                df_combined = df_combined.drop_duplicates(
                    subset=["game_id"], keep="first"
                )
            df_combined = df_combined.sort_values(
                by="created_at", ascending=False
            ).reset_index(drop=True)
            df_combined.to_csv(output_csv, index=False, encoding="utf-8-sig")
            print(
                f"[{datetime.now(timezone.utc)}] Combined and saved {len(df_combined)} games to '{output_csv}'."
            )
        else:
            print(
                f"[{datetime.now(timezone.utc)}] No new standard games to append."
            )

        if os.path.exists(temp_json):
            os.remove(temp_json)
            print(
                f"[{datetime.now(timezone.utc)}] Removed temporary JSON file '{temp_json}'."
            )

    if os.path.exists(rating_history_input_json):
        print(
            f"[{datetime.now(timezone.utc)}] Processing rating history for {username}..."
        )
        with open(rating_history_input_json, "r", encoding="utf-8") as f:
            raw_content = f.read().strip()
            rating_history = json.loads(raw_content) if raw_content else []

        t_rating = transform_rating_history(rating_history)
        if not t_rating.empty:
            t_rating.to_csv(
                rating_history_output_csv, index=False, encoding="utf-8-sig"
            )
            print(
                f"[{datetime.now(timezone.utc)}] Saved rating history to '{rating_history_output_csv}'."
            )
        else:
            print(
                f"[{datetime.now(timezone.utc)}] Rating history is empty for '{username}'; skipping rating CSV creation."
            )

    user_folder_id = get_or_create_user_folder(
        drive_service, username, drive_parent_folder_id
    )
    if os.path.exists(output_csv):
        upload_to_drive(drive_service, output_csv, user_folder_id, mimetype="text/csv")
    if os.path.exists(rating_history_output_csv):
        upload_to_drive(
            drive_service,
            rating_history_output_csv,
            user_folder_id,
            mimetype="text/csv",
        )


if __name__ == "__main__":
    main()