from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl
import streamlit as st

SRC_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_DATA_ROOTS = [
    PROJECT_ROOT / "src" / "data" / "market_data" / "kr" / "kospi200" / "1min",
    PROJECT_ROOT / "market_data" / "kr" / "kospi200" / "1min",
    PROJECT_ROOT / "data" / "market_data" / "kr" / "kospi200" / "1min",
]

SYMBOL_REFERENCE_PATHS = [
    PROJECT_ROOT / "data" / "reference" / "krx_all_symbols.json",
    PROJECT_ROOT / "data" / "reference" / "kospi200_symbols.json",
    SRC_DIR / "data" / "reference" / "krx_all_symbols.json",
    SRC_DIR / "data" / "reference" / "kospi200_symbols.json",
]


@dataclass(frozen=True)
class DataWindow:
    root: Path
    files: list[Path]
    frame: pl.DataFrame


def discover_data_roots() -> list[Path]:
    roots: list[Path] = []
    for root in DEFAULT_DATA_ROOTS:
        if root.exists() and root.is_dir() and any(root.glob("*.parquet")):
            roots.append(root)
    return roots


def list_available_dates(root: Path) -> list[date]:
    dates: list[date] = []
    for path in sorted(root.glob("*.parquet")):
        try:
            dates.append(date.fromisoformat(path.stem))
        except ValueError:
            continue
    return dates


@st.cache_data(show_spinner=False)
def load_ohlcv(root: str, symbols: tuple[str, ...], start_date: date, end_date: date) -> pl.DataFrame:
    root_path = Path(root)
    files: list[Path] = []

    for path in sorted(root_path.glob("*.parquet")):
        try:
            file_date = date.fromisoformat(path.stem)
        except ValueError:
            continue

        if start_date <= file_date <= end_date:
            files.append(path)

    if not files:
        return pl.DataFrame()

    frame = pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")

    if symbols:
        frame = frame.filter(pl.col("symbol").is_in(list(symbols)))

    required_columns = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    existing_columns = [column for column in required_columns if column in frame.columns]
    frame = frame.select(existing_columns)

    if {"symbol", "timestamp"}.issubset(frame.columns):
        frame = frame.sort(["symbol", "timestamp"])

    return frame


@st.cache_data(show_spinner=False)
def load_intraday_window(root: str, start_date: date, end_date: date) -> DataWindow:
    root_path = Path(root)
    files: list[Path] = []
    for path in sorted(root_path.glob("*.parquet")):
        try:
            file_date = date.fromisoformat(path.stem)
        except ValueError:
            continue

        if start_date <= file_date <= end_date:
            files.append(path)

    if not files:
        return DataWindow(root=root_path, files=[], frame=pl.DataFrame())

    frame = pl.concat(
        [pl.read_parquet(path) for path in files],
        how="vertical_relaxed",
    )

    if {"symbol", "timestamp"}.issubset(frame.columns):
        frame = frame.sort(["symbol", "timestamp"])

    return DataWindow(root=root_path, files=files, frame=frame)


@st.cache_data(show_spinner=False)
def load_symbol_name_map() -> dict[str, str]:
    symbol_names: dict[str, str] = {}

    for path in SYMBOL_REFERENCE_PATHS:
        if not path.exists():
            continue

        payload = json.loads(path.read_text(encoding="utf-8"))

        for component in payload.get("components", []):
            symbol = component.get("symbol")
            name = component.get("name")
            if symbol and name:
                symbol_names.setdefault(symbol_lookup_key(str(symbol)), str(name).strip())

        for item in payload.get("symbols", []):
            symbol = item.get("code") or item.get("symbol")
            name = item.get("ko_name") or item.get("name") or item.get("en_name")
            if symbol and name:
                symbol_names.setdefault(symbol_lookup_key(str(symbol)), str(name).strip())

    return symbol_names


def symbol_lookup_key(symbol: str) -> str:
    raw = str(symbol).split(".")[0].strip()
    digits = re.findall(r"\d{6}", raw)
    if digits:
        return digits[0]
    return raw.zfill(6) if raw.isdigit() else raw


def label_symbol(symbol: str, symbol_names: dict[str, str]) -> str:
    return symbol_names.get(symbol_lookup_key(symbol), str(symbol))


def summarize_period_returns(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty():
        return pl.DataFrame()

    required = {"timestamp", "symbol", "close"}
    if not required.issubset(frame.columns):
        missing = ", ".join(sorted(required - set(frame.columns)))
        raise ValueError(f"Missing required columns: {missing}")

    return (
        frame.sort(["symbol", "timestamp"])
        .group_by("symbol")
        .agg(
            pl.col("timestamp").first().alias("start_ts"),
            pl.col("timestamp").last().alias("end_ts"),
            pl.col("close").first().alias("start_close"),
            pl.col("close").last().alias("end_close"),
            pl.len().alias("bars"),
        )
        .with_columns(
            ((pl.col("end_close") / pl.col("start_close")) - 1)
            .mul(100)
            .alias("return_pct"),
            pl.when(pl.col("start_close") > 0)
            .then(pl.col("end_close") / pl.col("start_close"))
            .otherwise(None)
            .alias("return_multiple"),
        )
        .sort("return_pct", descending=True)
    )
