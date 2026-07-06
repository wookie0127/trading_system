from __future__ import annotations

from typing import Any

import polars as pl

REQUIRED_COLUMNS = ["date", "open", "high", "low", "close", "volume", "symbol"]


def normalize_ohlcv(raw: Any, symbol: str) -> pl.DataFrame:
    """Normalize yfinance or local OHLCV data to the harness schema."""
    if raw is None or len(raw) == 0:
        return pl.DataFrame(schema={col: pl.Utf8 for col in REQUIRED_COLUMNS})

    if isinstance(raw, pl.DataFrame):
        df = raw.clone()
    else:
        pandas_df = raw.copy()
        if hasattr(pandas_df, "columns") and hasattr(pandas_df.columns, "nlevels") and pandas_df.columns.nlevels > 1:
            pandas_df.columns = [str(col[0]).lower() for col in pandas_df.columns]
        else:
            pandas_df.columns = [str(col).lower().replace(" ", "_") for col in pandas_df.columns]

        if "date" not in pandas_df.columns:
            pandas_df = pandas_df.reset_index()
            pandas_df.columns = [str(col).lower().replace(" ", "_") for col in pandas_df.columns]
        df = pl.from_pandas(pandas_df)

    rename_map = {col: col.lower().replace(" ", "_") for col in df.columns}
    df = df.rename(rename_map)
    if "datetime" in df.columns and "date" not in df.columns:
        df = df.rename({"datetime": "date"})
    if "index" in df.columns and "date" not in df.columns:
        df = df.rename({"index": "date"})
    if "adj_close" in df.columns and "close" not in df.columns:
        df = df.rename({"adj_close": "close"})
    if "volume" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("volume"))

    missing = [col for col in ["date", "open", "high", "low", "close"] if col not in df.columns]
    if missing:
        raise ValueError(f"{symbol}: missing OHLCV columns: {missing}")

    return (
        df.select(["date", "open", "high", "low", "close", "volume"])
        .with_columns(
            pl.col("date").cast(pl.Datetime, strict=False).dt.date().alias("date"),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
            pl.lit(symbol.lower()).alias("symbol"),
        )
        .drop_nulls(["date", "open", "high", "low", "close"])
        .unique(subset=["date", "symbol"], keep="last")
        .sort(["symbol", "date"])
        .select(REQUIRED_COLUMNS)
    )
