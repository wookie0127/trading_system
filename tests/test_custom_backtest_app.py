from datetime import datetime, timedelta

import polars as pl

from backtest.engine import run_backtest, run_batch_backtests
from backtest.result import BacktestResult
from data.market_data import label_symbol, load_ohlcv
from strategies.registry import (
    STRATEGY_REGISTRY,
    get_strategy_spec,
    instantiate_strategy,
)


def test_run_backtest_returns_result_with_trades_and_equity():
    timestamps = [datetime(2026, 7, 1, 9) + timedelta(minutes=i) for i in range(8)]
    frame = pl.DataFrame(
        {
            "timestamp": timestamps,
            "symbol": ["005930"] * 8,
            "open": [100, 100, 100, 100, 100, 105, 95, 90],
            "high": [101, 101, 101, 101, 106, 106, 96, 91],
            "low": [99, 99, 99, 99, 99, 104, 94, 89],
            "close": [100, 100, 100, 100, 100, 105, 95, 90],
            "volume": [1000] * 8,
        }
    )

    result = run_backtest(
        frame,
        symbol="005930",
        stock_name="삼성전자",
        fast_window=2,
        slow_window=3,
        initial_balance=1_000_000,
        fee=0.0,
        slippage=0.0,
    )

    assert isinstance(result, BacktestResult)
    assert result.stock_name == "삼성전자"
    assert {"ma_fast", "ma_slow"}.issubset(result.ohlcv.columns)
    assert {"buy_signal", "sell_signal"}.issubset(result.signals.columns)
    assert result.equity_curve.height == 8
    assert {"total_return", "cagr", "mdd", "sharpe", "win_rate", "num_trades"}.issubset(
        result.metrics
    )


def test_run_batch_backtests_returns_compare_dataframe_sorted():
    timestamps = [datetime(2026, 7, 1, 9) + timedelta(minutes=i) for i in range(8)]
    frame = pl.concat(
        [
            pl.DataFrame(
                {
                    "timestamp": timestamps,
                    "symbol": [symbol] * 8,
                    "open": prices,
                    "high": prices,
                    "low": prices,
                    "close": prices,
                    "volume": [1000] * 8,
                }
            )
            for symbol, prices in {
                "005930": [100, 100, 100, 100, 100, 105, 95, 90],
                "000660": [100, 101, 102, 103, 104, 105, 106, 107],
            }.items()
        ],
        how="vertical",
    )

    compare = run_batch_backtests(
        frame,
        symbols=["005930", "000660"],
        symbol_names={"005930": "삼성전자", "000660": "SK하이닉스"},
        fast_window=2,
        slow_window=3,
        fee=0.0,
        slippage=0.0,
    )

    assert compare.get_column("stock_name").to_list() == ["SK하이닉스", "삼성전자"]
    assert {"total_return", "mdd", "sharpe", "num_trades"}.issubset(compare.columns)


def test_label_symbol_normalizes_kr_symbols():
    names = {"005930": "삼성전자", "000660": "SK하이닉스"}

    assert label_symbol("005930", names) == "삼성전자"
    assert label_symbol("000660.KS", names) == "SK하이닉스"
    assert label_symbol("A005930", names) == "삼성전자"


def test_strategy_registry_exposes_dynamic_parameter_spec():
    spec = get_strategy_spec("moving_average_cross")
    strategy = instantiate_strategy(spec, {})

    assert STRATEGY_REGISTRY["moving_average_cross"].name == "Moving Average Cross"
    assert [parameter.key for parameter in spec.parameters] == [
        "fast_window",
        "slow_window",
    ]
    assert hasattr(strategy, "generate_signals")


def test_load_ohlcv_loads_date_window_and_symbols(tmp_path):
    root = tmp_path / "1min"
    root.mkdir()
    day1 = pl.DataFrame(
        {
            "timestamp": [datetime(2026, 7, 1, 9), datetime(2026, 7, 1, 9)],
            "symbol": ["005930", "000660"],
            "open": [100.0, 200.0],
            "high": [101.0, 201.0],
            "low": [99.0, 199.0],
            "close": [100.0, 200.0],
            "volume": [1000, 2000],
        }
    )
    day2 = day1.with_columns(pl.lit(datetime(2026, 7, 2, 9)).alias("timestamp"))
    day1.write_parquet(root / "2026-07-01.parquet")
    day2.write_parquet(root / "2026-07-02.parquet")

    loaded = load_ohlcv(
        str(root), ("005930",), datetime(2026, 7, 1).date(), datetime(2026, 7, 2).date()
    )

    assert loaded.get_column("symbol").unique().to_list() == ["005930"]
    assert loaded.height == 2
