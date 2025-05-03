#!/usr/bin/env python3
"""
run_multiple_scripts.py

Runs fetch_games.py, fetch_rating_history.py, and preprocess_and_upload.py
for a given username in sequence.

Usage: python run_multiple_scripts.py <username>
"""

import subprocess
import sys

if len(sys.argv) != 2:
    raise RuntimeError("Usage: python run_multiple_scripts.py <username>")
USERNAME = sys.argv[1]

# List of scripts to run
scripts = [
    f"python fetch_games.py {USERNAME}",
    f"python fetch_rating_history.py {USERNAME}",
    f"python preprocess_and_upload.py {USERNAME}"
]

# Run each script sequentially
for script in scripts:
    print(f"Running: {script}")
    process = subprocess.run(script, shell=True, check=True)
    if process.returncode != 0:
        raise RuntimeError(f"Script {script} failed with exit code {process.returncode}")