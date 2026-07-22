import os
from pathlib import Path
from typing import Optional
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # Non-interactive backend for server/worker
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from loguru import logger
from src.core.config import CACHE_DIR


def generate_multi_timeframe_chart(
    df_15m: pd.DataFrame,
    df_1m: pd.DataFrame,
    output_path: Optional[Path] = None,
    action: str = "NO_TRADE",
    lookback: int = 40,
) -> Path:
    """
    Generates a double-panel chart containing:
    1. 15-minute BTCUSDT price chart (with MAs).
    2. 1-minute BTCUSDT price chart (with MAs).
    """
    out_dir = CACHE_DIR / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_path or (out_dir / "latest_multi_timeframe_chart.png")

    # Slice lookback windows
    df_sub_15m = df_15m.iloc[-lookback:].copy() if len(df_15m) >= lookback else df_15m.copy()
    df_sub_1m = df_1m.iloc[-lookback:].copy() if len(df_1m) >= lookback else df_1m.copy()

    if df_sub_15m.empty or df_sub_1m.empty:
        logger.warning("Empty dataframes for multi-timeframe chart generation")
        return target_path

    # Parse timestamps
    for df in [df_sub_15m, df_sub_1m]:
        if "timestamp_dt" not in df.columns:
            df["timestamp_dt"] = pd.to_datetime(df["timestamp"])

    # Compute MAs
    for df in [df_sub_15m, df_sub_1m]:
        df["ma20"] = df["close"].rolling(window=min(20, len(df)), min_periods=1).mean()
        df["ma50"] = df["close"].rolling(window=min(50, len(df)), min_periods=1).mean()

    # Create double-panel plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=False)
    plt.style.use("seaborn-v0_8-darkgrid" if "seaborn-v0_8-darkgrid" in plt.style.available else "default")

    # 1. 15-minute timeframe plot
    dates_15m = df_sub_15m["timestamp_dt"]
    ax1.plot(dates_15m, df_sub_15m["close"], label="15m Close", color="#1f77b4", linewidth=2)
    ax1.plot(dates_15m, df_sub_15m["ma20"], label="20 MA", color="#ff7f0e", linestyle="--")
    ax1.plot(dates_15m, df_sub_15m["ma50"], label="50 MA", color="#2ca02c", linestyle="--")
    latest_close = df_sub_15m["close"].iloc[-1]
    latest_time = dates_15m.iloc[-1].strftime("%Y-%m-%d %H:%M")

    action_color = "#28a745" if "LONG" in action else ("#dc3545" if "SHORT" in action else "#6c757d")
    ax1.set_title(
        f"BTCUSDT 15m Chart | Price: ${latest_close:,.2f} | Action: [{action}]",
        fontsize=12, fontweight="bold", color=action_color
    )
    ax1.set_ylabel("Price (USDT)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))

    # 2. 1-minute timeframe plot
    dates_1m = df_sub_1m["timestamp_dt"]
    ax2.plot(dates_1m, df_sub_1m["close"], label="1m Close", color="#17becf", linewidth=2)
    ax2.plot(dates_1m, df_sub_1m["ma20"], label="20 MA", color="#d62728", linestyle="--")
    ax2.plot(dates_1m, df_sub_1m["ma50"], label="50 MA", color="#9467bd", linestyle="--")
    latest_time_1m = dates_1m.iloc[-1].strftime("%H:%M:%S")

    ax2.set_title(f"BTCUSDT 1m Chart | Latest 1m: {latest_time_1m} UTC", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Price (USDT)")
    ax2.legend(loc="upper left")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M"))

    fig.autofmt_xdate()
    plt.tight_layout()

    plt.savefig(target_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Generated multi-timeframe chart: {target_path}")
    return target_path


def generate_snapshot_chart(
    df_candles: pd.DataFrame,
    current_idx: int,
    output_path: Optional[Path] = None,
    action: str = "NO_TRADE",
    lookback: int = 40,
) -> Path:
    """
    Backward compatible helper. Forwards single timeframe dataframe to a simple plot.
    """
    out_dir = CACHE_DIR / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_path or (out_dir / f"chart_{current_idx}.png")

    end_idx = current_idx + 1
    start_idx = max(0, end_idx - lookback)
    df_sub = df_candles.iloc[start_idx:end_idx].copy()

    if df_sub.empty:
        return target_path

    if "timestamp_dt" not in df_sub.columns:
        df_sub["timestamp_dt"] = pd.to_datetime(df_sub["timestamp"])

    df_sub["ma20"] = df_sub["close"].rolling(window=min(20, len(df_sub)), min_periods=1).mean()
    df_sub["ma50"] = df_sub["close"].rolling(window=min(50, len(df_sub)), min_periods=1).mean()

    fig, ax1 = plt.subplots(figsize=(10, 5))
    dates = df_sub["timestamp_dt"]
    ax1.plot(dates, df_sub["close"], label="Close Price", color="#1f77b4", linewidth=2)
    ax1.plot(dates, df_sub["ma20"], label="20 MA", color="#ff7f0e", linestyle="--")
    ax1.plot(dates, df_sub["ma50"], label="50 MA", color="#2ca02c", linestyle="--")

    latest_close = df_sub["close"].iloc[-1]
    ax1.set_title(f"BTCUSDT Chart | Price: ${latest_close:,.2f} | Action: {action}")
    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    plt.tight_layout()

    plt.savefig(target_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    return target_path
