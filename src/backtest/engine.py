from __future__ import annotations

import math
from statistics import fmean, pstdev
from typing import Any

import polars as pl

from backtest.result import BacktestResult
from strategies.moving_average import MovingAverageCrossStrategy


def run_backtest(
    df: pl.DataFrame,
    symbol: str,
    stock_name: str | None = None,
    fast_window: int = 20,
    slow_window: int = 60,
    initial_balance: float = 1_000_000.0,
    fee: float = 0.0005,
    slippage: float = 0.0005,
    trading_days_per_year: int = 252,
    strategy: Any | None = None,
    strategy_name: str | None = None,
) -> BacktestResult:
    ohlcv = _prepare_ohlcv(df)
    if ohlcv.is_empty():
        raise ValueError(f"No OHLCV rows for {symbol}.")

    strategy_instance = strategy or MovingAverageCrossStrategy(fast_window=fast_window, slow_window=slow_window)
    signals = _generate_strategy_signals(
        strategy=strategy_instance,
        ohlcv=ohlcv,
        fast_window=fast_window,
        slow_window=slow_window,
    )
    _validate_signal_columns(signals)
    trades, equity_curve = _simulate_long_only(
        signals=signals,
        symbol=symbol,
        initial_balance=initial_balance,
        transaction_cost=fee + slippage,
    )
    metrics = _build_metrics(
        equity_curve=equity_curve,
        trades=trades,
        initial_balance=initial_balance,
        trading_days_per_year=trading_days_per_year,
    )
    metrics.update(
        {
            "symbol": symbol,
            "stock_name": stock_name or symbol,
            "strategy": strategy_name or getattr(strategy_instance, "name", strategy_instance.__class__.__name__),
        }
    )

    return BacktestResult(
        symbol=symbol,
        stock_name=stock_name or symbol,
        ohlcv=_select_existing_columns(
            signals,
            ["timestamp", "symbol", "open", "high", "low", "close", "volume", "ma_fast", "ma_slow"],
        ),
        signals=_select_existing_columns(
            signals,
            ["timestamp", "symbol", "signal", "buy_signal", "sell_signal", "ma_fast", "ma_slow"],
        ),
        trades=trades,
        equity_curve=equity_curve,
        metrics=metrics,
    )


def run_batch_backtests(
    df: pl.DataFrame,
    symbols: list[str],
    symbol_names: dict[str, str],
    fast_window: int = 20,
    slow_window: int = 60,
    initial_balance: float = 1_000_000.0,
    fee: float = 0.0005,
    slippage: float = 0.0005,
    trading_days_per_year: int = 252,
    strategy: Any | None = None,
    strategy_name: str | None = None,
) -> pl.DataFrame:
    rows: list[dict[str, float | int | str]] = []
    for symbol in symbols:
        symbol_df = df.filter(pl.col("symbol") == symbol)
        if symbol_df.is_empty():
            continue
        result = run_backtest(
            symbol_df,
            symbol=symbol,
            stock_name=symbol_names.get(symbol, symbol),
            initial_balance=initial_balance,
            fee=fee,
            slippage=slippage,
            trading_days_per_year=trading_days_per_year,
            strategy=strategy,
            strategy_name=strategy_name,
        )
        rows.append(result.metrics)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["sharpe", "mdd", "total_return", "num_trades"],
        descending=[True, True, True, True],
    )


def _prepare_ohlcv(df: pl.DataFrame) -> pl.DataFrame:
    required = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(sorted(missing))}")
    return (
        df.with_columns(
            pl.col("timestamp").cast(pl.Datetime, strict=False),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
            pl.col("volume").cast(pl.Float64, strict=False),
        )
        .drop_nulls(["timestamp", "close"])
        .sort("timestamp")
    )


def _simulate_long_only(
    signals: pl.DataFrame,
    symbol: str,
    initial_balance: float,
    transaction_cost: float,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    cash = float(initial_balance)
    quantity = 0.0
    position = 0
    entry_value = 0.0
    entry_price = 0.0
    trades: list[dict[str, object]] = []
    equity_rows: list[dict[str, object]] = []

    for row in signals.iter_rows(named=True):
        timestamp = row["timestamp"]
        close = float(row["close"])

        if bool(row["buy_signal"]) and position == 0:
            fee_paid = cash * transaction_cost
            investable_cash = cash - fee_paid
            quantity = investable_cash / close if close else 0.0
            cash = 0.0
            position = 1
            entry_value = quantity * close
            entry_price = close
            trades.append(
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "side": "BUY",
                    "price": close,
                    "quantity": quantity,
                    "fee": fee_paid,
                    "pnl": 0.0,
                    "return": 0.0,
                    "reason": row.get("buy_reason") or row.get("reason") or "buy_signal",
                }
            )
        elif bool(row["sell_signal"]) and position == 1:
            gross_value = quantity * close
            fee_paid = gross_value * transaction_cost
            cash = gross_value - fee_paid
            pnl = cash - entry_value
            trade_return = (close / entry_price - 1) - (2 * transaction_cost) if entry_price else 0.0
            trades.append(
                {
                    "timestamp": timestamp,
                    "symbol": symbol,
                    "side": "SELL",
                    "price": close,
                    "quantity": quantity,
                    "fee": fee_paid,
                    "pnl": pnl,
                    "return": trade_return,
                    "reason": row.get("sell_reason") or row.get("reason") or "sell_signal",
                }
            )
            quantity = 0.0
            position = 0
            entry_value = 0.0
            entry_price = 0.0

        equity = cash + quantity * close
        equity_rows.append(
            {
                "timestamp": timestamp,
                "symbol": symbol,
                "equity": equity,
                "return": equity / initial_balance - 1,
                "position": position,
            }
        )

    equity_curve = pl.DataFrame(equity_rows)
    if not equity_curve.is_empty():
        equity_curve = equity_curve.with_columns(
            pl.col("equity").cum_max().alias("peak"),
        ).with_columns(
            (pl.col("equity") / pl.col("peak") - 1).alias("drawdown"),
        )
    return pl.DataFrame(trades), equity_curve


# Helper functions for strategy signal handling and column selection/validation
def _generate_strategy_signals(
    strategy: Any,
    ohlcv: pl.DataFrame,
    fast_window: int,
    slow_window: int,
) -> pl.DataFrame:
    """Generate signals while supporting both parameterized strategy instances and legacy strategies."""
    try:
        return strategy.generate_signals(ohlcv)
    except TypeError:
        return strategy.generate_signals(ohlcv, fast_window=fast_window, slow_window=slow_window)


def _validate_signal_columns(signals: pl.DataFrame) -> None:
    required = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "buy_signal", "sell_signal"}
    missing = required - set(signals.columns)
    if missing:
        raise ValueError(f"Strategy output is missing required columns: {', '.join(sorted(missing))}")


def _select_existing_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    existing = [column for column in columns if column in frame.columns]
    return frame.select(existing)


def _build_metrics(
    equity_curve: pl.DataFrame,
    trades: pl.DataFrame,
    initial_balance: float,
    trading_days_per_year: int,
) -> dict[str, float | int]:
    if equity_curve.is_empty():
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "mdd": 0.0,
            "sharpe": 0.0,
            "win_rate": 0.0,
            "num_trades": 0,
        }

    final_equity = float(equity_curve.get_column("equity")[-1])
    total_return = final_equity / initial_balance - 1
    periods = max(equity_curve.height, 1)
    years = max(periods / trading_days_per_year, 1 / trading_days_per_year)
    cagr = (1 + total_return) ** (1 / years) - 1 if total_return > -1 else -1.0
    returns = equity_curve.get_column("equity").pct_change().fill_null(0.0).to_list()
    std = pstdev(returns) if len(returns) > 1 else 0.0
    sharpe = math.sqrt(trading_days_per_year) * fmean(returns) / std if std else 0.0
    sells = trades.filter(pl.col("side") == "SELL") if not trades.is_empty() else trades
    num_trades = sells.height
    win_rate = float((sells.get_column("pnl") > 0).mean()) if num_trades else 0.0
    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "mdd": float(equity_curve.get_column("drawdown").min()),
        "sharpe": float(sharpe),
        "win_rate": win_rate,
        "num_trades": int(num_trades),
        "final_equity": final_equity,
    }
