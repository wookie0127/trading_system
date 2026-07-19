from datetime import datetime, timedelta
import polars as pl
from data.resampler import resample_ohlcv

def test_resample_ohlcv_aggregates_correctly():
    # Create 10 minutes of 1m candles for symbol TEST
    base_time = datetime(2026, 7, 1, 9, 0)
    timestamps = [base_time + timedelta(minutes=i) for i in range(10)]
    df = pl.DataFrame({
        "timestamp": timestamps,
        "symbol": ["TEST"] * 10,
        "open": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0],
        "high": [12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0],
        "low": [9.0, 10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0],
        "close": [11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0],
        "volume": [100.0] * 10
    })

    # Resample to 5m
    resampled_5m = resample_ohlcv(df, "5m")
    
    assert resampled_5m.height == 2
    assert resampled_5m.columns == ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    
    # First 5m candle (index 0): 09:00 to 09:04
    c1 = resampled_5m.row(0, named=True)
    assert c1["timestamp"] == datetime(2026, 7, 1, 9, 0)
    assert c1["open"] == 10.0  # open of 09:00
    assert c1["high"] == 16.0 # max of high in 09:00-09:04 [12, 13, 14, 15, 16]
    assert c1["low"] == 9.0   # min of low in 09:00-09:04 [9, 10, 11, 12, 13]
    assert c1["close"] == 15.0 # close of 09:04
    assert c1["volume"] == 500.0 # sum of volume
    
    # Second 5m candle (index 1): 09:05 to 09:09
    c2 = resampled_5m.row(1, named=True)
    assert c2["timestamp"] == datetime(2026, 7, 1, 9, 5)
    assert c2["open"] == 15.0  # open of 09:05
    assert c2["high"] == 21.0 # max of high in 09:05-09:09 [17, 18, 19, 20, 21]
    assert c2["low"] == 14.0   # min of low in 09:05-09:09 [14, 15, 16, 17, 18]
    assert c2["close"] == 20.0 # close of 09:09
    assert c2["volume"] == 500.0 # sum of volume

def test_resample_ohlcv_handles_empty_dataframe():
    df = pl.DataFrame(schema={
        "timestamp": pl.Datetime,
        "symbol": pl.String,
        "open": pl.Float64,
        "high": pl.Float64,
        "low": pl.Float64,
        "close": pl.Float64,
        "volume": pl.Float64
    })
    
    resampled = resample_ohlcv(df, "5m")
    assert resampled.is_empty()
