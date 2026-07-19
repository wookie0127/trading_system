from datetime import date

import polars as pl

from trading_harness.strategies.trend_following import SimpleTrendFollowingStrategy
from trading_harness.strategies.us_market_shock_inverse import (
    USMarketShockInverseStrategy,
)


def test_us_market_shock_inverse_signal():
    features = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 1)],
            "symbol": ["qqq", "vix"],
            "return_1d": [-0.02, 0.06],
        }
    )

    signals = USMarketShockInverseStrategy().generate(features)

    row = signals.to_dicts()[0]
    assert row["signal"] == 1
    assert row["symbol"] == "kospi200_inverse"


def test_trend_following_signal():
    features = pl.DataFrame(
        {
            "date": [date(2024, 1, 1), date(2024, 1, 2)],
            "symbol": ["qqq", "qqq"],
            "close": [100, 105],
            "ma_20": [101, 103],
            "ma_60": [102, 100],
            "return_1d": [0.0, 0.05],
        }
    )

    signals = SimpleTrendFollowingStrategy(["qqq"]).generate(features)

    assert signals.get_column("signal").to_list() == [0, 1]
