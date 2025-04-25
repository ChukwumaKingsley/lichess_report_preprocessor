# Lichess Player Analysis

A lightweight Python pipeline to fetch, preprocess, and publish your Lichess games and rating history for dashboard‐ready CSVs. Automatically uploads results to a Google Drive folder for easy Excel/Power BI consumption.

---

## 📦 Repository Structure

```text
├── fetch_pgn.py             # Download user games in PGN or JSON format
├── fetch_rating_history.py  # Download Lichess rating history by category
├── preprocess_and_upload.py # Preprocess games & ratings; upload CSVs to Google Drive
├── requirements.txt         # Python dependencies
└── .env.example             # Environment variables template
