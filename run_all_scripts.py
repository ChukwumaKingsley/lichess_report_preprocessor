#!/usr/bin/env python3
"""
run_multiple_scripts.py

Runs multiple Python scripts in the same directory.
Can use a hardcoded list of scripts or dynamically find all .py files.
Logs execution status and errors.
"""

import os
import subprocess
import sys
from datetime import datetime

def get_current_directory():
    """Get the directory of this script."""
    return os.path.dirname(os.path.abspath(__file__))

def get_python_files(directory, exclude_file):
    """Get all .py files in the directory, excluding the specified file."""
    return [
        f for f in os.listdir(directory)
        if f.endswith('.py') and f != exclude_file and os.path.isfile(os.path.join(directory, f))
    ]

def run_script(script_path):
    """Run a Python script and return the result."""
    print(f"[{datetime.now()}] Running {script_path}...")
    try:
        # Use sys.executable to ensure the same Python version is used
        result = subprocess.run(
            [sys.executable, script_path],
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
    # Directory of this script
    current_dir = get_current_directory()
    this_script = os.path.basename(__file__)

    # Option 1: Hardcode specific scripts to run (uncomment and edit as needed)
    scripts_to_run = [
        "fetch_games.py",
        "fetch_rating_history.py",
        "preprocess_and_upload.py",
    ]

    # Option 2: Dynamically find all .py files (comment out if using hardcoded list)
    scripts_to_run = get_python_files(current_dir, this_script)

    if not scripts_to_run:
        print(f"[{datetime.now()}] No Python scripts found to run.")
        return

    print(f"[{datetime.now()}] Found {len(scripts_to_run)} scripts to run: {scripts_to_run}")

    # Run each script
    for script in scripts_to_run:
        script_path = os.path.join(current_dir, script)
        run_script(script_path)

    print(f"[{datetime.now()}] All scripts completed.")

if __name__ == "__main__":
    main()