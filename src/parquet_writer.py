"""
Parquet write utility for market data storage.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

MARKET_DATA_DIR = Path(__file__).parents[1] / "data" / "market_data"

_PATHS = {
    "nasdaq_1min":           MARKET_DATA_DIR / "us" / "nasdaq" / "1min",
    "us_stock_1min":         MARKET_DATA_DIR / "us" / "stock" / "1min",
    "us_daily":              MARKET_DATA_DIR / "us" / "daily",
    "global_daily":          MARKET_DATA_DIR / "global" / "daily",
    "kospi200_1min":         MARKET_DATA_DIR / "kr" / "kospi200" / "1min",
    "kr_stock_1min":         MARKET_DATA_DIR / "kr" / "stock" / "1min",
    "kr_stock_daily":        MARKET_DATA_DIR / "kr" / "stock" / "daily",
    "investor_flow_daily":   MARKET_DATA_DIR / "kr" / "investor_flow" / "daily",
    "investor_flow_10min":   MARKET_DATA_DIR / "kr" / "investor_flow" / "10min",
    "kospi200_daily_yf":     MARKET_DATA_DIR / "kr" / "kospi200" / "daily",
    "kospi200_components":   MARKET_DATA_DIR / "metadata" / "kospi200_components",
    "us_stock_daily":        MARKET_DATA_DIR / "us" / "stock" / "daily",
}


def get_dir(key: str) -> Path:
    if key not in _PATHS:
        raise KeyError(f"Unknown data key: {key!r}. Choose from {list(_PATHS)}")
    return _PATHS[key]


def write_parquet(df: pd.DataFrame, dest: Path) -> None:
    """Write *df* to *dest*, deduplicating against any existing file."""
    if df.empty:
        logger.warning(f"Empty DataFrame — skipping write to {dest}")
        return

    dest.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False)

    if dest.exists():
        existing = pq.read_table(dest)
        table = pa.concat_tables([existing, table])
        # Drop duplicates in Arrow via pandas round-trip
        merged = table.to_pandas()
        merged = merged.drop_duplicates(subset=["timestamp", "symbol"])
        table = pa.Table.from_pandas(merged, preserve_index=False)

    pq.write_table(table, dest, compression="snappy")
    logger.info(f"Saved {table.num_rows} rows → {dest}")


def get_last_sync_stats(dest: Path) -> dict[str, pd.Timestamp]:
    """이미 저장된 파일에서 종목별 마지막 수집 시점을 반환합니다."""
    if not dest.exists():
        return {}
    try:
        # timestamp와 symbol 컬럼만 읽어서 효율적으로 집계
        df = pd.read_parquet(dest, columns=["timestamp", "symbol"])
        if df.empty:
            return {}
        return df.groupby("symbol")["timestamp"].max().to_dict()
    except Exception:
        return {}


def is_symbol_in_data(dest: Path, symbol: str) -> bool:
    """지정된 파일(dest)에 해당 종목(symbol) 데이터가 이미 존재하는지 확인합니다."""
    if not dest.exists():
        return False
    try:
        # symbol 컬럼만 읽어서 존재 여부 확인 (효율적)
        df = pd.read_parquet(dest, columns=["symbol"])
        return symbol in df["symbol"].unique()
    except Exception:
        return False


def daily_path(key: str, date_str: str) -> Path:
    """Return path like <key_dir>/<date_str>.parquet (date_str: YYYY-MM-DD)."""
    return get_dir(key) / f"{date_str}.parquet"
