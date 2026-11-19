from pathlib import Path

import pandas as pd
import pytest

from fetch_games import compute_max_created_at_ms
from preprocess_and_upload import (
    escape_drive_query,
    format_time_control,
    transform_games_records,
    transform_rating_history,
    validate_username,
)
from run_all_scripts import validate_username as validate_runner_username


def test_username_validation_rejects_shell_and_path_traversal():
    assert validate_username("Real_Vick") == "Real_Vick"
    assert validate_runner_username("Wizard-King") == "Wizard-King"

    for bad in [
        "",
        "a",
        "../etc/passwd",
        "user; rm -rf /",
        "user$(whoami)",
        "user`id`",
        "user/name",
        "user' OR '1'='1",
        "a" * 35,
    ]:
        with pytest.raises(ValueError):
            validate_username(bad)
        with pytest.raises(ValueError):
            validate_runner_username(bad)


def test_escape_drive_query():
    assert escape_drive_query("O'Connor\\Test") == "O\\'Connor\\\\Test"


def test_format_time_control_handles_subminute_90s_and_standard():
    assert format_time_control({"initial": 180, "increment": 0}) == "3+0"
    assert format_time_control({"initial": 60, "increment": 1}) == "1+1"
    assert format_time_control({"initial": 30, "increment": 0}) == "1/2+0"
    assert format_time_control({"initial": 45, "increment": 0}) == "3/4+0"
    # BUG-07 regression: 90s must not be truncated to 1+0
    assert format_time_control({"initial": 90, "increment": 0}) == "3/2+0"
    assert format_time_control({"initial": 0, "increment": 1}) == "0+1"
    assert format_time_control(None) is None


def test_transform_games_records_tournament_per_row_and_missing_fields():
    raw_games = [
        {
            "id": "g1",
            "rated": True,
            "variant": "standard",
            "speed": "blitz",
            "createdAt": 1700000000000,
            "lastMoveAt": 1700000100000,
            "status": "mate",
            "source": "arena",
            "tournament": "tourney123",
            "players": {
                "white": {"user": {"name": "Real_Vick"}, "rating": 1500, "ratingDiff": 6},
                "black": {"user": {"name": "Rival1"}, "rating": 1490, "ratingDiff": -6},
            },
            "winner": "white",
            "opening": {"eco": "B12", "name": "Caro-Kann Defense", "ply": 5},
            "clock": {"initial": 180, "increment": 2},
            "moves": "e4 c6 d4 d5 e5",
        },
        {
            "id": "g2",
            "rated": False,
            "variant": "standard",
            "speed": "correspondence",
            "createdAt": 1700000200000,
            "lastMoveAt": 1700000300000,
            "status": "draw",
            "source": "friend",
            # No tournament, no opening, no clock, no moves, unnamed friend opponent
            "players": {
                "white": {"rating": 1500},
                "black": {"user": {"name": "real_vick"}, "rating": 1506},
            },
        },
        {
            "id": "g3_variant_ignored",
            "rated": True,
            "variant": "chess960",
            "speed": "blitz",
        },
    ]

    df = transform_games_records(raw_games, "Real_Vick")
    assert len(df) == 2

    row1 = df.iloc[0]
    row2 = df.iloc[1]
    # BUG-01 regression: row1 is tournament=True, row2 MUST be tournament=False
    assert bool(row1["tournament"]) is True
    assert bool(row2["tournament"]) is False

    assert row1["played_as"] == "white"
    assert row1["result"] == "win"
    assert row1["move_count"] == 5
    assert row1["turns"] == 3

    assert row2["played_as"] == "black"
    assert row2["result"] == "draw"
    assert row2["opponent_name"] == "Unnamed"
    assert row2["time_control"] == "daily"
    assert pd.isna(row2["move_count"])


def test_transform_rating_history_empty_and_duplicate_dates():
    # Empty rating history must not raise KeyError
    empty_df = transform_rating_history([])
    assert empty_df.empty

    # Duplicate dates in same category must not raise ValueError on pivot
    dup_history = [
        {
            "name": "Blitz",
            "points": [
                [2025, 0, 10, 1500],
                [2025, 0, 10, 1525],  # Second entry on same day 2025-01-10
                [2025, 0, 12, 1540],
            ],
        }
    ]
    df = transform_rating_history(dup_history)
    assert len(df) == 3  # Jan 10, Jan 11 (ffill), Jan 12
    jan10 = df[df["date"] == pd.Timestamp("2025-01-10")].iloc[0]
    assert jan10["Blitz"] == 1525


def test_compute_max_created_at_ms_handles_empty_and_nat(tmp_path: Path):
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("game_id,created_at\n", encoding="utf-8")
    assert compute_max_created_at_ms(str(empty_csv)) == 0

    nat_csv = tmp_path / "nat.csv"
    nat_csv.write_text("game_id,created_at\ng1,not-a-date\n", encoding="utf-8")
    assert compute_max_created_at_ms(str(nat_csv)) == 0

    valid_csv = tmp_path / "valid.csv"
    valid_csv.write_text(
        "game_id,created_at\ng1,2025-01-15 12:00:00\n", encoding="utf-8"
    )
    assert compute_max_created_at_ms(str(valid_csv)) > 0
