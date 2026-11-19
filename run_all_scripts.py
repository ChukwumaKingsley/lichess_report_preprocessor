#!/usr/bin/env python3
"""
run_all_scripts.py

Runs fetch_games.py, fetch_rating_history.py, and preprocess_and_upload.py
for a given username in sequence without shell invocation.

Usage: python run_all_scripts.py <username>
"""

import re
import subprocess
import sys

USERNAME_RE = re.compile(r"^[A-Za-z0-9_-]{2,30}$")


def validate_username(username: str) -> str:
    cleaned = (username or "").strip()
    if not USERNAME_RE.fullmatch(cleaned):
        raise ValueError(
            f"Invalid Lichess username '{username}'. Must be 2-30 alphanumeric, '_', or '-' characters."
        )
    return cleaned


def main() -> None:
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python run_all_scripts.py <username>")
    username = validate_username(sys.argv[1])

    scripts = [
        "fetch_games.py",
        "fetch_rating_history.py",
        "preprocess_and_upload.py",
    ]

    for script_name in scripts:
        cmd = [sys.executable, script_name, username]
        print(f"Running: {' '.join(cmd)}")
        process = subprocess.run(cmd, shell=False, check=True)
        if process.returncode != 0:
            raise RuntimeError(
                f"Script {script_name} failed with exit code {process.returncode}"
            )


if __name__ == "__main__":
    main()