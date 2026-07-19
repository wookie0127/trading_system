import polars as pl
from pathlib import Path
from loguru import logger


def resample_ohlcv(df: pl.DataFrame, every: str) -> pl.DataFrame:
    """
    Resamples a Polars DataFrame of 1-minute OHLCV data to a higher timeframe.
    Assumes df has columns: timestamp (Datetime), symbol, open, high, low, close, volume.

    Parameters:
    - df: pl.DataFrame with OHLCV data.
    - every: Polars duration string (e.g., '5m', '15m', '30m', '1h', '4h', '1d').
    """
    if df.is_empty():
        return df.clone()

    # Ensure timestamp is datetime and sorted
    df = df.with_columns(pl.col("timestamp").cast(pl.Datetime))
    df = df.sort(["symbol", "timestamp"])

    resampled = df.group_by_dynamic(
        "timestamp", every=every, group_by="symbol", closed="left", label="left"
    ).agg(
        [
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        ]
    )

    # Clean up and ensure columns are in the correct order
    cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    resampled = resampled.select(cols).drop_nulls(["close"])
    return resampled


def resample_and_save(
    input_path: str | Path, output_dir: str | Path, timeframes: list[str]
) -> None:
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        logger.error(f"Input file {input_path} does not exist.")
        return

    df = pl.read_parquet(input_path)
    symbol = df["symbol"][0] if df.height > 0 else "UNKNOWN"

    for tf in timeframes:
        logger.info(f"Resampling {symbol} 1m to {tf}...")
        resampled = resample_ohlcv(df, tf)
        out_path = output_dir / f"{symbol}_{tf}.parquet"
        resampled.write_parquet(out_path)
        logger.success(f"Saved {resampled.height} candles of {tf} to {out_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Resample OHLCV 1m data")
    parser.add_argument("--input", default="data/raw/BTCUSDT_1m.parquet")
    parser.add_argument("--output_dir", default="data/processed")
    parser.add_argument(
        "--timeframes", nargs="+", default=["5m", "15m", "30m", "1h", "4h", "1d"]
    )
    args = parser.parse_args()

    resample_and_save(args.input, args.output_dir, args.timeframes)
