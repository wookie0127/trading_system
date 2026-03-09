"""
Data validation for collected market data.
"""

from __future__ import annotations

import pandas as pd
from loguru import logger


class ValidationError(Exception):
    pass


def validate_intraday(df: pd.DataFrame, symbol: str = "") -> pd.DataFrame:
    """
    Validate intraday price data and return a cleaned copy.

    Checks (per spec §10):
    - duplicate timestamps
    - missing (NaT) timestamps
    - price <= 0
    - volume < 0
    """
    tag = f"[{symbol}] " if symbol else ""
    issues: list[str] = []

    if df.empty:
        logger.warning(f"{tag}Empty DataFrame.")
        return df

    df = df.copy()

    # --- missing timestamps ---
    if "timestamp" in df.columns:
        missing_ts = df["timestamp"].isna().sum()
        if missing_ts:
            issues.append(f"{missing_ts} rows with missing timestamp")
            df = df.dropna(subset=["timestamp"])

    # --- duplicate timestamps ---
    if "timestamp" in df.columns:
        dup = df.duplicated(subset=["timestamp", "symbol"] if "symbol" in df.columns else ["timestamp"])
        n_dup = dup.sum()
        if n_dup:
            issues.append(f"{n_dup} duplicate timestamp rows")
            df = df[~dup]

    # --- price <= 0 ---
    price_cols = [c for c in ("open", "high", "low", "close") if c in df.columns]
    for col in price_cols:
        bad = (df[col] <= 0).sum()
        if bad:
            issues.append(f"{bad} rows with {col} <= 0")
            df = df[df[col] > 0]

    # --- volume < 0 ---
    if "volume" in df.columns:
        bad_vol = (df["volume"] < 0).sum()
        if bad_vol:
            issues.append(f"{bad_vol} rows with volume < 0")
            df = df[df["volume"] >= 0]

    if issues:
        logger.warning(f"{tag}Validation issues: {'; '.join(issues)}")
    else:
        logger.info(f"{tag}Validation passed ({len(df)} rows).")

    return df


def validate_investor_flow(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate daily investor flow data and return a cleaned copy.
    """
    issues: list[str] = []

    if df.empty:
        logger.warning("Empty investor flow DataFrame.")
        return df

    df = df.copy()

    # --- missing date ---
    if "date" in df.columns:
        missing = df["date"].isna().sum()
        if missing:
            issues.append(f"{missing} rows with missing date")
            df = df.dropna(subset=["date"])

    # --- duplicates ---
    key_cols = [c for c in ("date", "symbol") if c in df.columns]
    if key_cols:
        dup = df.duplicated(subset=key_cols)
        n_dup = dup.sum()
        if n_dup:
            issues.append(f"{n_dup} duplicate rows")
            df = df[~dup]

    # --- volume < 0 ---
    if "volume" in df.columns:
        bad = (df["volume"] < 0).sum()
        if bad:
            issues.append(f"{bad} rows with volume < 0")
            df = df[df["volume"] >= 0]

    if issues:
        logger.warning(f"Investor flow validation issues: {'; '.join(issues)}")
    else:
        logger.info(f"Investor flow validation passed ({len(df)} rows).")

    return df
