"""
Core configuration module for path management and environment settings.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Project Root Directory
PROJECT_DIR = Path(os.getenv("PROJECT_DIR", Path(__file__).resolve().parents[2]))

# Load .env from PROJECT_DIR if present
load_dotenv(PROJECT_DIR / ".env")

# Core Data Directories
DATA_DIR = Path(os.getenv("DATA_DIR", PROJECT_DIR / "data"))
DATA_RAW_DIR = DATA_DIR / "raw"
DATA_PROCESSED_DIR = DATA_DIR / "processed"
DATA_REFERENCE_DIR = DATA_DIR / "reference"
DATA_TELEGRAM_DIR = DATA_DIR / "follow_telegram_leading"

# Output & System Directories
LOGS_DIR = Path(os.getenv("LOGS_DIR", PROJECT_DIR / "logs"))
CACHE_DIR = Path(os.getenv("CACHE_DIR", PROJECT_DIR / "cache"))
REPORTS_DIR = Path(os.getenv("REPORTS_DIR", PROJECT_DIR / "reports"))
OBSIDIAN_DIR = Path(os.getenv("OBSIDIAN_DIR", PROJECT_DIR / "obsidian"))

# Primary Database Path
DB_PATH = Path(os.getenv("DB_PATH", PROJECT_DIR / "trading_system.db"))

# API Keys (Prioritize GEMINI_API as requested, with fallback to GEMINI_API_KEY / GOOGLE_API_KEY)
GEMINI_API_KEY = (
    os.getenv("GEMINI_API")
    or os.getenv("GEMINI_API_KEY")
    or os.getenv("GOOGLE_API_KEY")
)
