#!/usr/bin/env python3
"""
fetch_rating_history.py

Fetches Lichess rating history for a user in JSON format and saves it to a local file.

Usage: python fetch_rating_history.py <username>
"""

import os
import re
import sys
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

USERNAME_RE = re.compile(r"^[A-Za-z0-9_-]{2,30}$")


def validate_username(username: str) -> str:
    cleaned = (username or "").strip()
    if not USERNAME_RE.fullmatch(cleaned):
        raise ValueError(f"Invalid Lichess username: '{username}'")
    return cleaned


def main() -> None:
    load_dotenv()
    lichess_token = os.getenv("LICHESS_TOKEN")
    if not lichess_token:
        raise RuntimeError("LICHESS_TOKEN not set in .env")

    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python fetch_rating_history.py <username>")
    username = validate_username(sys.argv[1])

    player_data_folder = os.path.join(os.getcwd(), "Player Data")
    player_folder = os.path.join(player_data_folder, username)
    os.makedirs(player_folder, exist_ok=True)
    output_json = os.path.join(player_folder, f"rating_history_{username}.json")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {lichess_token}",
    }
    url = f"https://lichess.org/api/user/{username}/rating-history"

    print(f"[{datetime.now(timezone.utc)}] Fetching rating history for {username}...")
    resp = requests.get(url, headers=headers, stream=True, timeout=30)
    resp.raise_for_status()

    with open(output_json, "w", encoding="utf-8") as f:
        for line in resp.iter_lines():
            if not line:
                continue
            decoded_line = line.decode("utf-8").strip()
            if decoded_line:
                f.write(decoded_line + "\n")

    print(
        f"[{datetime.now(timezone.utc)}] Saved rating history to '{output_json}' locally."
    )


if __name__ == "__main__":
    main()