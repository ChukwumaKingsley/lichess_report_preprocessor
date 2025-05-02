#!/usr/bin/env python3
"""
run_multiple_scripts.py

Runs multiple Python scripts in the same directory with a provided username.
Scripts are hardcoded to ensure specific execution order.
Logs execution status and errors.

Usage: python run_multiple_scripts.py <username>
"""

import os
import subprocess
import sys
from datetime import datetime

def get_current_directory():
    """Get the directory of this script."""
    return os.path.dirname(os.path.abspath(__file__))

def run_script(script_path, username):
    """Run a Python script with the given username and return the result."""
    print(f"[{datetime.now()}] Running {script_path} for user {username}...")
    try:
        # Use sys.executable to ensure the same Python version is used
        # Pass the username as a command-line argument to the script
        result = subprocess.run(
            [sys.executable, script_path, username],
            capture_output=True,
            text=True,
            check=False
        )
        if result.returncode == 0:
            print(f"[{datetime.now()}] {script_path} completed successfully.")
            if result.stdout:
                print(f"Output:\n{result.stdout}")
        else:
            print(f"[{datetime.now()}] {script_path} failed with exit code {result.returncode}.")
            if result.stderr:
                print(f"Error:\n{result.stderr}")
        return result.returncode
    except Exception as e:
        print(f"[{datetime.now()}] Exception while running {script_path}: {str(e)}")
        return 1

def main():
    # Check for username argument
    if len(sys.argv) != 2:
        raise RuntimeError("Usage: python run_multiple_scripts.py <username>")
    username = sys.argv[1]

    # Directory of this script
    current_dir = get_current_directory()

    # Hardcode specific scripts to run in the desired order
    scripts_to_run = [
        "fetch_games.py",  # Assuming fetch_games.py is the renamed fetch_json.py
        "fetch_rating_history.py",
        "preprocess_and_upload.py",
    ]

    # Verify that all scripts exist
    missing_scripts = [script for script in scripts_to_run if not os.path.exists(os.path.join(current_dir, script))]
    if missing_scripts:
        raise RuntimeError(f"[{datetime.now()}] The following scripts were not found: {missing_scripts}")

    print(f"[{datetime.now()}] Found {len(scripts_to_run)} scripts to run: {scripts_to_run}")

    # Run each script with the username
    for script in scripts_to_run:
        script_path = os.path.join(current_dir, script)
        exit_code = run_script(script_path, username)
        if exit_code != 0:
            print(f"[{datetime.now()}] Stopping execution due to failure in {script}.")
            sys.exit(exit_code)

    print(f"[{datetime.now()}] All scripts completed successfully.")

if __name__ == "__main__":
    main()