import yfinance as yf
from datetime import timedelta
from pathlib import Path


def check_interval(symbol, interval, period):
    print(
        f"\n[Yahoo Finance] Checking {symbol} for interval {interval} over period {period}..."
    )
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False)
        if df.empty:
            print("  FAILED: No data returned.")
            return None

        # Flatten MultiIndex if necessary
        if hasattr(df.columns, "levels"):
            df.columns = [col[0] for col in df.columns]

        df = df.dropna()
        if df.empty:
            print("  FAILED: Data is all NaN.")
            return None

        start_date = df.index.min()
        end_date = df.index.max()
        count = len(df)

        print(f"  SUCCESS: {count} rows found.")
        print(f"  Range: {start_date} to {end_date}")

        # Check for gaps
        if interval == "1d":
            diff = df.index.to_series().diff().max()
            print(f"  Max gap: {diff}")
        else:
            # Check if index has date attribute (it should for DatetimeIndex)
            df["date"] = df.index.date
            max_intra_gap = timedelta(0)
            for date, group in df.groupby("date"):
                if len(group) > 1:
                    intra_diff = group.index.to_series().diff().max()
                    if intra_diff > max_intra_gap:
                        max_intra_gap = intra_diff
            print(f"  Max intra-day gap: {max_intra_gap}")

        return df
    except Exception as e:
        print(f"  ERROR: {e}")
        return None


def check_local_data(symbol):
    print(f"\n[Local Data] Checking {symbol}...")
    base_dir = Path("src/data/market_data/us/stock")

    # Check daily
    daily_dir = base_dir / "daily"
    if daily_dir.exists():
        files = sorted(list(daily_dir.glob("*.parquet")))
        if files:
            print(
                f"  Daily data found: {len(files)} files ({files[0].name} to {files[-1].name})"
            )
        else:
            print("  Daily data: No files found.")
    else:
        print("  Daily data: Directory not found.")

    # Check 1min
    min_dir = base_dir / "1min"
    if min_dir.exists():
        files = sorted(list(min_dir.glob("*.parquet")))
        if files:
            print(
                f"  1min data found: {len(files)} files ({files[0].name} to {files[-1].name})"
            )
        else:
            print("  1min data: No files found.")
    else:
        print("  1min data: Directory not found.")


if __name__ == "__main__":
    symbol = "QQQ"
    period = "3mo"
    intervals = ["1m", "5m", "15m", "1d"]

    check_local_data(symbol)

    for interval in intervals:
        check_interval(symbol, interval, period)
