import os
import json
import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from datetime import datetime

# Load environment variables
load_dotenv()
USERNAME = os.getenv("LICHESS_USERNAME")
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")  # Path to JSON key file
DRIVE_FOLDER_ID = os.getenv("GOOGLE_DRIVE_FOLDER_ID")  # Google Drive folder ID

if not USERNAME:
    raise RuntimeError("LICHESS_USERNAME not set in .env")
if not SERVICE_ACCOUNT_FILE:
    raise RuntimeError("GOOGLE_SERVICE_ACCOUNT_JSON not set in .env")
if not DRIVE_FOLDER_ID:
    raise RuntimeError("GOOGLE_DRIVE_FOLDER_ID not set in .env")

# Filenames based on username
GAMES_INPUT_JSON = f"games_{USERNAME}.json"
GAMES_OUTPUT_CSV = f"games_{USERNAME}.csv"
RATING_HISTORY_INPUT_JSON = f"rating_history_{USERNAME}.json"
RATING_HISTORY_OUTPUT_CSV = f"rating_history_{USERNAME}.csv"

# --------------------------------
# STEP 1: Read and preprocess data
# --------------------------------
# Read NDJSON file
games = []
with open(GAMES_INPUT_JSON, "r", encoding="utf-8") as f:
    for line in f:
        games.append(json.loads(line))

# Convert to DataFrame
df = pd.DataFrame(games)

# Helper functions
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
            # If the initial time is less than 60 seconds, show as a fraction of a minute
            if initial < 60:
                # Simplify the fraction (initial/60) and get the numerator/denominator
                numerator = initial
                denominator = 60
                # Reduce the fraction
                from math import gcd
                common_divisor = gcd(numerator, denominator)
                numerator //= common_divisor
                denominator //= common_divisor
                # Return the fraction format for initial time
                return f"{numerator}/{denominator}+{increment}"
            else:
                # Convert seconds to minutes for times >= 60 seconds
                initial_minutes = initial // 60
                return f"{initial_minutes}+{increment}"
    return None

# Apply transformations
df['played_as'], df['opponent_color'] = zip(*df.apply(get_sides, axis=1))
df['player_name'] = USERNAME
df['opponent_name'] = df.apply(lambda r: get_player_name(r['players'][r['opponent_color']]), axis=1)

# For 'ai' source, set opponent to 'Lichess Stockfish'
df['opponent_name'] = df.apply(lambda r: 'Lichess Stockfish' if r['source'] == 'ai' else r['opponent_name'], axis=1)

df['player_rating'] = df.apply(lambda r: get_rating(r['players'][r['played_as']], 'rating'), axis=1)
df['player_rating_diff'] = df.apply(lambda r: get_rating(r['players'][r['played_as']], 'ratingDiff'), axis=1)
df['opponent_rating'] = df.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'rating'), axis=1)
df['opponent_rating_diff'] = df.apply(lambda r: get_rating(r['players'][r['opponent_color']], 'ratingDiff'), axis=1)
df['result'] = df.apply(map_result, axis=1)
df['opening_eco'] = df['opening'].apply(lambda o: o.get('eco') if isinstance(o, dict) else None)
df['opening_name'] = df['opening'].apply(lambda o: o.get('name') if isinstance(o, dict) else None)
df['opening_ply'] = df['opening'].apply(lambda o: o.get('ply') if isinstance(o, dict) else None)

# Time control column
df['time_control'] = df['clock'].apply(format_time_control)

# Timestamps and metadata
df['game_id'] = df['id']
df['rated'] = df['rated']
df['speed'] = df['speed']
df['created_at'] = pd.to_datetime(df['createdAt'], unit='ms')
df['last_move_at'] = pd.to_datetime(df['lastMoveAt'], unit='ms')
df['status'] = df['status']
df['source'] = df['source']
df['tournament'] = df.get('tournament') is not None

# Filter out non-standard games and drop the 'variant' column
df = df[df['variant'] == 'standard']  # Keep only standard games
df = df.drop(columns=['variant'])  # Remove the variant column

# Handle moves and turns
if 'moves' in df.columns:
    df['move_count'] = df['moves'].apply(lambda m: len(m.split()) if isinstance(m, str) else None)
    df['turns'] = df['move_count'].apply(lambda mc: (mc + 1) // 2 if isinstance(mc, int) else None)
else:
    df['move_count'] = None
    df['turns'] = None

# Clean up rows with invalid or missing data
df.loc[df['opponent_name'].isnull() & (df['source'] == 'friend'), 'opponent_name'] = 'Unnamed'

# Convert 'correspondence' games with no time control to 'daily'
df.loc[df['time_control'].isnull() & (df['speed'] == 'correspondence'), 'time_control'] = 'daily'

# Convert time control from 'second+second' to 'minute+second' for other games
df['time_control'] = df['time_control'].apply(
    lambda x: x.replace("s", "m") if isinstance(x, str) and "+" in x else x
)

# Reorder columns for final output
columns = [
    'game_id', 'rated', 'speed', 'created_at', 'last_move_at', 'status', 'source',
    'player_name', 'played_as', 'opponent_name', 'opponent_color',
    'player_rating', 'player_rating_diff', 'opponent_rating', 'opponent_rating_diff',
    'result', 'opening_eco', 'opening_name', 'opening_ply', 'tournament',
    'time_control', 'move_count', 'turns'
]
final_df = df[columns]

# Save to CSV
to_save_df = final_df.copy()
to_save_df.to_csv(GAMES_OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"Saved preprocessed game data to '{GAMES_OUTPUT_CSV}'")


# Read rating history JSON file
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

# Pivot so each category is a column, indexed by date
every = rating_df.pivot(index="date", columns="category", values="rating")

# Reindex to include all dates in range
all_dates = pd.date_range(start=every.index.min(), end=every.index.max())
every = every.reindex(all_dates)

# Forward fill missing values by column
every_filled = every.ffill()

# Combine to keep original NaNs prior to first known rating
rating_final = every.combine_first(every_filled)

# Reset index and rename columns
rating_final = rating_final.reset_index().rename(columns={"index": "date"})

# Ensure 'date' is datetime
if not pd.api.types.is_datetime64_any_dtype(rating_final['date']):
    rating_final['date'] = pd.to_datetime(rating_final['date'])

# Sort descending by date
t_rating = rating_final.sort_values(by='date', ascending=False).reset_index(drop=True)

# Save rating history CSV with BOM
rating_csv = t_rating.copy()
rating_csv.to_csv(RATING_HISTORY_OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"Saved rating history to '{RATING_HISTORY_OUTPUT_CSV}'")

# --------------------------------
# STEP 3: Upload both CSVs to Google Drive
# --------------------------------
SCOPES = ['https://www.googleapis.com/auth/drive.file']
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
service = build('drive', 'v3', credentials=creds)

def find_file_id(name, folder_id):
    q = f"name='{name}' and '{folder_id}' in parents and trashed=false"
    res = service.files().list(q=q, fields='files(id,name)').execute()
    files = res.get('files', [])
    return files[0]['id'] if files else None

def upload_or_update_file(filename):
    file_id = find_file_id(filename, DRIVE_FOLDER_ID)
    media = MediaFileUpload(filename, mimetype='text/csv', resumable=True)
    if file_id:
        service.files().update(fileId=file_id, media_body=media).execute()
        print(f"Updated file on Drive: {filename} (ID: {file_id})")
    else:
        file_metadata = {'name': filename, 'parents': [DRIVE_FOLDER_ID]}
        new_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print(f"Uploaded new file to Drive: {filename} (ID: {new_file.get('id')})")

upload_or_update_file(GAMES_OUTPUT_CSV)
upload_or_update_file(RATING_HISTORY_OUTPUT_CSV)