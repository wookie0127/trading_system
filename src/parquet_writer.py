"""
Parquet write utility for market data storage.
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

MARKET_DATA_DIR = Path(__file__).parents[1] / "market_data"

_PATHS = {
    "nasdaq_1min":           MARKET_DATA_DIR / "us" / "nasdaq" / "1min",
    "kospi200_1min":         MARKET_DATA_DIR / "kr" / "kospi200" / "1min",
    "investor_flow_daily":   MARKET_DATA_DIR / "kr" / "investor_flow" / "daily",
    "kospi200_components":   MARKET_DATA_DIR / "metadata" / "kospi200_components",
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
        merged = merged.drop_duplicates()
        table = pa.Table.from_pandas(merged, preserve_index=False)

    pq.write_table(table, dest, compression="snappy")
    logger.info(f"Saved {table.num_rows} rows → {dest}")


def daily_path(key: str, date_str: str) -> Path:
    """Return path like <key_dir>/<date_str>.parquet (date_str: YYYY-MM-DD)."""
    return get_dir(key) / f"{date_str}.parquet"
