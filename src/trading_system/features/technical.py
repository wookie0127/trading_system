import numpy as np
import pandas as pd


def compute_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Computes technical indicators without look-ahead bias.
    Expects df with columns: ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    """
    df = df.copy()

    # EMAs
    df["ema_20"] = df["close"].ewm(span=20, adjust=False).mean()
    df["ema_50"] = df["close"].ewm(span=50, adjust=False).mean()
    df["ema_100"] = df["close"].ewm(span=100, adjust=False).mean()
    df["ema_200"] = df["close"].ewm(span=200, adjust=False).mean()

    # RSI 14
    delta = df["close"].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / (loss.replace(0, np.nan))
    df["rsi_14"] = 100 - (100 / (1 + rs))
    df["rsi_14"] = df["rsi_14"].fillna(50)

    # MACD (12, 26, 9)
    ema_12 = df["close"].ewm(span=12, adjust=False).mean()
    ema_26 = df["close"].ewm(span=26, adjust=False).mean()
    df["macd"] = ema_12 - ema_26
    df["macd_signal"] = df["macd"].ewm(span=9, adjust=False).mean()
    df["macd_histogram"] = df["macd"] - df["macd_signal"]

    # ATR 14
    high_low = df["high"] - df["low"]
    high_close = (df["high"] - df["close"].shift()).abs()
    low_close = (df["low"] - df["close"].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    df["atr_14"] = tr.rolling(window=14).mean().fillna(0)
    df["atr_ratio"] = df["atr_14"] / df["close"]

    # Bollinger Bands 20, 2std
    ma_20 = df["close"].rolling(window=20).mean()
    std_20 = df["close"].rolling(window=20).std()
    df["bollinger_upper"] = ma_20 + (std_20 * 2)
    df["bollinger_middle"] = ma_20
    df["bollinger_lower"] = ma_20 - (std_20 * 2)
    band_width = df["bollinger_upper"] - df["bollinger_lower"]
    df["bollinger_position"] = np.where(
        band_width > 0, (df["close"] - df["bollinger_middle"]) / (band_width / 2), 0.0
    )

    # Volume Z-score
    vol_mean = df["volume"].rolling(window=20).mean()
    vol_std = df["volume"].rolling(window=20).std().replace(0, np.nan)
    df["volume_ma"] = vol_mean
    df["volume_zscore"] = ((df["volume"] - vol_mean) / vol_std).fillna(0.0)

    return df
