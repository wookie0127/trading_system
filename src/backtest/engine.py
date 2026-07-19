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
    stop_loss_pct: float | None = None,
    take_profit_pct: float | None = None,
) -> BacktestResult:
    ohlcv = _prepare_ohlcv(df)
    if ohlcv.is_empty():
        raise ValueError(f"No OHLCV rows for {symbol}.")

    strategy_instance = strategy or MovingAverageCrossStrategy(
        fast_window=fast_window, slow_window=slow_window
    )
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
        stop_loss_pct=stop_loss_pct,
        take_profit_pct=take_profit_pct,
    )
    metrics = _build_metrics(
        equity_curve=equity_curve,
        trades=trades,
        initial_balance=initial_balance,
        trading_days_per_year=trading_days_per_year,
        ohlcv=ohlcv,
    )
    metrics.update(
        {
            "symbol": symbol,
            "stock_name": stock_name or symbol,
            "strategy": strategy_name
            or getattr(strategy_instance, "name", strategy_instance.__class__.__name__),
        }
    )

    return BacktestResult(
        symbol=symbol,
        stock_name=stock_name or symbol,
        ohlcv=_select_existing_columns(
            signals,
            [
                "timestamp",
                "symbol",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "ma_fast",
                "ma_slow",
            ],
        ),
        signals=_select_existing_columns(
            signals,
            [
                "timestamp",
                "symbol",
                "signal",
                "buy_signal",
                "sell_signal",
                "ma_fast",
                "ma_slow",
            ],
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
    stop_loss_pct: float | None = None,
    take_profit_pct: float | None = None,
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
            fast_window=fast_window,
            slow_window=slow_window,
            initial_balance=initial_balance,
            fee=fee,
            slippage=slippage,
            trading_days_per_year=trading_days_per_year,
            strategy=strategy,
            strategy_name=strategy_name,
            stop_loss_pct=stop_loss_pct,
            take_profit_pct=take_profit_pct,
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
    stop_loss_pct: float | None = None,
    take_profit_pct: float | None = None,
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
        high = float(row.get("high", close))
        low = float(row.get("low", close))

        # Check exit conditions if currently in a position
        if position == 1:
            stop_loss_hit = False
            take_profit_hit = False

            stop_loss_price = (
                entry_price * (1.0 - stop_loss_pct)
                if stop_loss_pct is not None
                else 0.0
            )
            take_profit_price = (
                entry_price * (1.0 + take_profit_pct)
                if take_profit_pct is not None
                else float("inf")
            )

            if stop_loss_pct is not None and low <= stop_loss_price:
                stop_loss_hit = True
            if take_profit_pct is not None and high >= take_profit_price:
                take_profit_hit = True

            exit_price = None
            exit_reason = None

            if stop_loss_hit and take_profit_hit:
                # Default to Stop Loss as conservative assumption
                exit_price = stop_loss_price
                exit_reason = "Stop Loss"
            elif stop_loss_hit:
                exit_price = stop_loss_price
                exit_reason = "Stop Loss"
            elif take_profit_hit:
                exit_price = take_profit_price
                exit_reason = "Take Profit"
            elif bool(row["sell_signal"]):
                exit_price = close
                exit_reason = (
                    row.get("sell_reason") or row.get("reason") or "sell_signal"
                )

            if exit_price is not None:
                gross_value = quantity * exit_price
                fee_paid = gross_value * transaction_cost
                cash = gross_value - fee_paid
                pnl = cash - entry_value
                trade_return = (
                    (exit_price / entry_price - 1) - (2 * transaction_cost)
                    if entry_price
                    else 0.0
                )

                trades.append(
                    {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "side": "SELL",
                        "price": exit_price,
                        "quantity": quantity,
                        "fee": fee_paid,
                        "pnl": pnl,
                        "return": trade_return,
                        "reason": exit_reason,
                    }
                )
                quantity = 0.0
                position = 0
                entry_value = 0.0
                entry_price = 0.0

        # Check entry condition if FLAT
        if position == 0 and bool(row["buy_signal"]):
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
                    "reason": row.get("buy_reason")
                    or row.get("reason")
                    or "buy_signal",
                }
            )

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
    import inspect

    sig = inspect.signature(strategy.generate_signals)
    kwargs = {}
    if "fast_window" in sig.parameters:
        kwargs["fast_window"] = fast_window
    if "slow_window" in sig.parameters:
        kwargs["slow_window"] = slow_window
    if "short_ma_period" in sig.parameters:
        kwargs["short_ma_period"] = fast_window
    if "long_ma_period" in sig.parameters:
        kwargs["long_ma_period"] = slow_window

    return strategy.generate_signals(ohlcv, **kwargs)


def _validate_signal_columns(signals: pl.DataFrame) -> None:
    required = {
        "timestamp",
        "symbol",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "buy_signal",
        "sell_signal",
    }
    missing = required - set(signals.columns)
    if missing:
        raise ValueError(
            f"Strategy output is missing required columns: {', '.join(sorted(missing))}"
        )


def _select_existing_columns(frame: pl.DataFrame, columns: list[str]) -> pl.DataFrame:
    existing = [column for column in columns if column in frame.columns]
    return frame.select(existing)


def _build_metrics(
    equity_curve: pl.DataFrame,
    trades: pl.DataFrame,
    initial_balance: float,
    trading_days_per_year: int,
    ohlcv: pl.DataFrame | None = None,
) -> dict[str, float | int | str]:
    if equity_curve.is_empty():
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "mdd": 0.0,
            "sharpe": 0.0,
            "win_rate": 0.0,
            "num_trades": 0,
            "profit_factor": 0.0,
            "average_holding_time": "0 mins",
            "benchmark_return": 0.0,
        }

    final_equity = float(equity_curve.get_column("equity")[-1])
    total_return = final_equity / initial_balance - 1

    # Calculate years dynamically using timestamps
    start_dt = equity_curve["timestamp"][0]
    end_dt = equity_curve["timestamp"][-1]
    years = (
        (end_dt - start_dt).total_seconds() / (365.25 * 24 * 3600)
        if end_dt > start_dt
        else 0.0
    )
    years = max(years, 1 / 365.25)
    cagr = (1 + total_return) ** (1 / years) - 1 if total_return > -1 else -1.0

    # Sharpe Ratio
    returns = equity_curve.get_column("equity").pct_change().fill_null(0.0).to_list()
    std = pstdev(returns) if len(returns) > 1 else 0.0
    if std > 0:
        bars_per_year = len(equity_curve) / years
        sharpe = math.sqrt(bars_per_year) * fmean(returns) / std
    else:
        sharpe = 0.0

    sells = trades.filter(pl.col("side") == "SELL") if not trades.is_empty() else trades
    num_trades = sells.height
    win_rate = float((sells.get_column("pnl") > 0).mean()) if num_trades else 0.0

    # Profit Factor
    if num_trades > 0:
        pnl_list = sells["pnl"].to_list()
        gross_profit = sum(p for p in pnl_list if p > 0)
        gross_loss = sum(abs(p) for p in pnl_list if p < 0)
        profit_factor = (
            gross_profit / gross_loss
            if gross_loss > 0
            else (999.0 if gross_profit > 0 else 0.0)
        )
    else:
        profit_factor = 0.0

    # Average Holding Time
    holding_seconds = []
    trade_list = trades.to_dicts() if not trades.is_empty() else []
    for i in range(0, len(trade_list) - 1, 2):
        if i + 1 < len(trade_list):
            buy_t = trade_list[i]["timestamp"]
            sell_t = trade_list[i + 1]["timestamp"]
            holding_seconds.append((sell_t - buy_t).total_seconds())

    avg_seconds = fmean(holding_seconds) if holding_seconds else 0.0
    if avg_seconds >= 86400:
        average_holding_time = f"{avg_seconds / 86400:.2f} days"
    elif avg_seconds >= 3600:
        average_holding_time = f"{avg_seconds / 3600:.2f} hours"
    else:
        average_holding_time = f"{avg_seconds / 60:.2f} mins"

    # Benchmark Return
    if ohlcv is not None and not ohlcv.is_empty():
        benchmark_return = float(ohlcv["close"][-1] / ohlcv["close"][0] - 1)
    else:
        benchmark_return = 0.0

    return {
        "total_return": float(total_return),
        "cagr": float(cagr),
        "mdd": float(equity_curve.get_column("drawdown").min())
        if not equity_curve.is_empty()
        else 0.0,
        "sharpe": float(sharpe),
        "win_rate": win_rate,
        "num_trades": int(num_trades),
        "final_equity": final_equity,
        "profit_factor": float(profit_factor),
        "average_holding_time": average_holding_time,
        "benchmark_return": float(benchmark_return),
    }
