from datetime import date

import polars as pl

from data.market_data import load_intraday_window, summarize_period_returns


def test_streamlit_app_helpers_load_and_summarize_period_returns(tmp_path):
    root = tmp_path / "kospi200" / "1min"
    root.mkdir(parents=True)

    day1 = pl.DataFrame(
        {
            "timestamp": [date(2026, 6, 28), date(2026, 6, 28)],
            "symbol": ["AAA.KS", "BBB.KS"],
            "open": [100.0, 200.0],
            "high": [102.0, 202.0],
            "low": [98.0, 198.0],
            "close": [100.0, 200.0],
            "volume": [100.0, 200.0],
        }
    )
    day2 = pl.DataFrame(
        {
            "timestamp": [date(2026, 6, 29), date(2026, 6, 29)],
            "symbol": ["AAA.KS", "BBB.KS"],
            "open": [110.0, 180.0],
            "high": [112.0, 182.0],
            "low": [108.0, 178.0],
            "close": [110.0, 180.0],
            "volume": [120.0, 220.0],
        }
    )
    day1.write_parquet(root / "2026-06-28.parquet")
    day2.write_parquet(root / "2026-06-29.parquet")

    window = load_intraday_window(str(root), date(2026, 6, 28), date(2026, 6, 29))
    assert len(window.files) == 2
    assert window.frame.height == 4

    summary = summarize_period_returns(window.frame)
    assert summary.height == 2
    aaa = summary.filter(pl.col("symbol") == "AAA.KS").row(0, named=True)
    bbb = summary.filter(pl.col("symbol") == "BBB.KS").row(0, named=True)
    assert round(aaa["return_pct"], 2) == 10.00
    assert round(bbb["return_pct"], 2) == -10.00
