from pathlib import Path
from datetime import date, timedelta

import polars as pl
import yaml

from trading_harness.agents.backtest_agent import BacktestAgent
from trading_harness.agents.feature_agent import FeatureAgent
from trading_harness.agents.report_agent import ReportAgent
from trading_harness.agents.strategy_agent import StrategyAgent


def _write_yaml(path: Path, payload: dict) -> None:
    path.write_text(yaml.safe_dump(payload), encoding="utf-8")


def _raw(symbol: str, start: int = 100) -> pl.DataFrame:
    dates = [date(2024, 1, 1) + timedelta(days=i) for i in range(90)]
    close = list(range(start, start + len(dates)))
    return pl.DataFrame(
        {
            "date": dates,
            "open": close,
            "high": [value + 1 for value in close],
            "low": [value - 1 for value in close],
            "close": close,
            "volume": 1000,
            "symbol": symbol,
        }
    )


def test_agent_pipeline_with_local_parquet(tmp_path):
    raw_dir = tmp_path / "raw"
    features_dir = tmp_path / "features"
    signals_dir = tmp_path / "signals"
    backtests_dir = tmp_path / "backtests"
    reports_dir = tmp_path / "reports"
    raw_dir.mkdir()

    for symbol, start in [
        ("qqq", 100),
        ("vix", 20),
        ("usdkrw", 1300),
        ("btc", 40000),
        ("eth", 2000),
        ("kospi200_inverse", 5000),
    ]:
        _raw(symbol, start).write_parquet(raw_dir / f"{symbol}.parquet")

    tickers = {
        "data": {
            "raw_dir": str(raw_dir),
            "features_dir": str(features_dir),
            "signals_dir": str(signals_dir),
            "backtests_dir": str(backtests_dir),
            "reports_dir": str(reports_dir),
        },
        "market_data": {
            "tickers": {
                "qqq": "QQQ",
                "vix": "^VIX",
                "usdkrw": "KRW=X",
                "btc": "BTC-USD",
                "eth": "ETH-USD",
                "kospi200_inverse": "114800.KS",
            }
        },
    }
    strategies = {
        "strategies": {
            "us_market_shock_inverse": {
                "enabled": True,
                "output": str(signals_dir / "shock.parquet"),
                "qqq_return_threshold": -0.015,
                "vix_change_threshold": 0.05,
            },
            "trend_following": {
                "enabled": True,
                "symbols": ["qqq", "btc", "eth"],
                "output": str(signals_dir / "trend.parquet"),
            },
        }
    }
    backtest = {
        "backtest": {
            "fee_rate": 0.0005,
            "slippage_rate": 0.0005,
            "trading_days_per_year": 252,
            "output": str(backtests_dir / "result.parquet"),
        }
    }
    ticker_config = tmp_path / "tickers.yaml"
    strategy_config = tmp_path / "strategies.yaml"
    backtest_config = tmp_path / "backtest.yaml"
    _write_yaml(ticker_config, tickers)
    _write_yaml(strategy_config, strategies)
    _write_yaml(backtest_config, backtest)

    feature_path = FeatureAgent(ticker_config).run()
    signal_paths = StrategyAgent(ticker_config, strategy_config).run()
    backtest_path = BacktestAgent(ticker_config, strategy_config, backtest_config).run()
    report_path = ReportAgent(ticker_config, strategy_config, backtest_config).run()

    assert feature_path.exists()
    assert set(signal_paths) == {"us_market_shock_inverse", "trend_following"}
    assert backtest_path.exists()
    assert report_path.exists()
    assert "# Daily Trading Research Report" in report_path.read_text(encoding="utf-8")
