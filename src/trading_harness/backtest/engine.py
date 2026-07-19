from __future__ import annotations

import polars as pl

from trading_harness.backtest.metrics import summarize_returns


class LongOnlyBacktestEngine:
    def __init__(
        self,
        fee_rate: float = 0.0005,
        slippage_rate: float = 0.0005,
        trading_days_per_year: int = 252,
    ) -> None:
        self.transaction_cost = fee_rate + slippage_rate
        self.trading_days_per_year = trading_days_per_year

    def run(
        self, prices: pl.DataFrame, signals: pl.DataFrame, strategy_name: str
    ) -> pl.DataFrame:
        if signals.is_empty():
            return pl.DataFrame()

        results: list[dict[str, object]] = []
        for symbol in signals.select("symbol").unique().to_series().to_list():
            price_df = prices.filter(pl.col("symbol") == symbol).select(
                ["date", "close"]
            )
            if price_df.is_empty():
                continue
            signal_df = signals.filter(pl.col("symbol") == symbol).select(
                ["date", "signal"]
            )
            merged = (
                price_df.join(signal_df, on="date", how="left")
                .with_columns(pl.col("signal").fill_null(0).cast(pl.Int8))
                .sort("date")
            )
            metrics = self._run_symbol(merged)
            metrics.update({"strategy": strategy_name, "symbol": symbol})
            results.append(metrics)

        return pl.DataFrame(results) if results else pl.DataFrame()

    def _run_symbol(self, df: pl.DataFrame) -> dict[str, float | int]:
        closes = df.get_column("close").to_list()
        raw_signals = [int(value or 0) for value in df.get_column("signal").to_list()]
        positions = [0] + raw_signals[:-1]

        strategy_returns: list[float] = []
        previous_position = 0
        previous_close = None
        equity = 1.0
        entry_equity = 1.0
        trade_returns: list[float] = []

        for close, position in zip(closes, positions, strict=True):
            asset_return = (
                0.0
                if previous_close in (None, 0)
                else float(close / previous_close - 1)
            )
            cost = abs(position - previous_position) * self.transaction_cost
            strategy_return = position * asset_return - cost
            strategy_returns.append(strategy_return)

            equity *= 1 + strategy_return
            if previous_position == 0 and position == 1:
                entry_equity = equity
            elif previous_position == 1 and position == 0:
                trade_returns.append(equity / entry_equity - 1)

            previous_position = position
            previous_close = close

        if previous_position == 1:
            trade_returns.append(equity / entry_equity - 1)

        return summarize_returns(
            strategy_returns,
            positions,
            trade_returns,
            trading_days_per_year=self.trading_days_per_year,
        )
