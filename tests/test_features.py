from datetime import date, timedelta

import polars as pl

from trading_harness.features.technical_indicators import build_feature_dataset


def test_build_feature_dataset_adds_required_features():
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(80)]
    frames = []
    for symbol, start in [("qqq", 100), ("vix", 20), ("usdkrw", 1300), ("btc", 40000)]:
        frames.append(
            pl.DataFrame(
                {
                    "date": dates,
                    "open": range(start, start + len(dates)),
                    "high": range(start + 1, start + len(dates) + 1),
                    "low": range(start - 1, start + len(dates) - 1),
                    "close": range(start, start + len(dates)),
                    "volume": 1000,
                    "symbol": symbol,
                }
            )
        )
    features = build_feature_dataset(pl.concat(frames, how="vertical"))

    qqq = features.filter(pl.col("symbol") == "qqq").tail(1).to_dicts()[0]
    assert "return_1d" in features.columns
    assert "volatility_20d" in features.columns
    assert "rsi_14" in features.columns
    assert "vix_change_1d" in features.columns
    assert "usdkrw_change_1d" in features.columns
    assert "btc_return_1d" in features.columns
    assert qqq["ma_60"] is not None
