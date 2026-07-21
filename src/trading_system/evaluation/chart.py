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


def generate_snapshot_chart(
    df_candles: pd.DataFrame,
    current_idx: int,
    output_path: Optional[Path] = None,
    action: str = "NO_TRADE",
    lookback: int = 40,
) -> Path:
    """
    Generates a technical analysis chart PNG image for BTCUSDT snapshot.
    Plots Price, Moving Averages (20MA, 50MA), and RSI/Volume.
    """
    out_dir = CACHE_DIR / "charts"
    out_dir.mkdir(parents=True, exist_ok=True)
    target_path = output_path or (out_dir / f"chart_{current_idx}.png")

    # Slice lookback window
    end_idx = current_idx + 1
    start_idx = max(0, end_idx - lookback)
    df_sub = df_candles.iloc[start_idx:end_idx].copy()

    if df_sub.empty:
        logger.warning("Empty dataframe for chart generation")
        return target_path

    # Ensure timestamp conversion
    if "timestamp_dt" not in df_sub.columns:
        df_sub["timestamp_dt"] = pd.to_datetime(df_sub["timestamp"])

    # Calculate Moving Averages
    df_sub["ma20"] = (
        df_sub["close"].rolling(window=min(20, len(df_sub)), min_periods=1).mean()
    )
    df_sub["ma50"] = (
        df_sub["close"].rolling(window=min(50, len(df_sub)), min_periods=1).mean()
    )

    # Setup Plot Style
    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(10, 6), gridspec_kw={"height_ratios": [3, 1]}, sharex=True
    )
    plt.style.use(
        "seaborn-v0_8-darkgrid"
        if "seaborn-v0_8-darkgrid" in plt.style.available
        else "default"
    )

    # Plot Price & Moving Averages
    dates = df_sub["timestamp_dt"]
    ax1.plot(dates, df_sub["close"], label="Close Price", color="#1f77b4", linewidth=2)
    ax1.plot(
        dates,
        df_sub["ma20"],
        label="20 MA",
        color="#ff7f0e",
        linestyle="--",
        linewidth=1.5,
    )
    ax1.plot(
        dates,
        df_sub["ma50"],
        label="50 MA",
        color="#2ca02c",
        linestyle="--",
        linewidth=1.5,
    )

    # Latest Candle Annotations
    latest_close = df_sub["close"].iloc[-1]
    latest_time = dates.iloc[-1].strftime("%Y-%m-%d %H:%M")

    # Color code action banner
    action_color = (
        "#28a745"
        if "LONG" in action
        else ("#dc3545" if "SHORT" in action else "#6c757d")
    )
    ax1.set_title(
        f"BTCUSDT 4H Chart | Time: {latest_time} UTC | Action: [{action}] | Price: ${latest_close:,.2f}",
        fontsize=12,
        fontweight="bold",
        color=action_color,
        pad=10,
    )
    ax1.set_ylabel("Price (USDT)")
    ax1.legend(loc="upper left")
    ax1.grid(True, alpha=0.3)

    # Plot Volume Subplot
    if "volume" in df_sub.columns:
        colors = [
            "#28a745" if c >= o else "#dc3545"
            for c, o in zip(df_sub["close"], df_sub["open"])
        ]
        ax2.bar(dates, df_sub["volume"], color=colors, alpha=0.6, width=0.08)
        ax2.set_ylabel("Volume")
        ax2.grid(True, alpha=0.3)

    # Format Date Axis
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M"))
    fig.autofmt_xdate()
    plt.tight_layout()

    plt.savefig(target_path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Generated chart image: {target_path}")

    return target_path
