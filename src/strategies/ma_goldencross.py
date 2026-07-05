"""
단기 이동평균선이 장기 이동평균선을 상향 돌파 -> 매수
단기 이동평균선이 장기 이동평균선을 하향 돌파 -> 매도
"""

import polars as pl


short_ma_period = 20
long_ma_period = 60


def add_signals(df: pl.DataFrame, short_ma_period: int = short_ma_period, long_ma_period: int = long_ma_period) -> pl.DataFrame:
    sort_columns = ["timestamp"]
    if "symbol" in df.columns:
        sort_columns = ["symbol", "timestamp"]

    df = df.sort(sort_columns)
    partition = ["symbol"] if "symbol" in df.columns else None
    short_ma = pl.col("close").rolling_mean(short_ma_period)
    long_ma = pl.col("close").rolling_mean(long_ma_period)
    if partition:
        short_ma = short_ma.over(partition)
        long_ma = long_ma.over(partition)

    df = df.with_columns(
        short_ma.alias("short_ma"),
        long_ma.alias("long_ma"),
    )
    short_prev = pl.col("short_ma").shift(1)
    long_prev = pl.col("long_ma").shift(1)
    if partition:
        short_prev = short_prev.over(partition)
        long_prev = long_prev.over(partition)

    df = df.with_columns(
        short_prev.alias("short_prev"),
        long_prev.alias("long_prev"),
    )
    df = df.with_columns(
        (
            (pl.col("short_prev") <= pl.col("long_prev"))
            & (pl.col("short_ma") > pl.col("long_ma"))
        ).fill_null(False).alias("buy_signal")
    )
    df = df.with_columns(
        (
            (pl.col("short_prev") >= pl.col("long_prev"))
            & (pl.col("short_ma") < pl.col("long_ma"))
        ).fill_null(False).alias("sell_signal")
    )
    return df


def backtest_ma_cross(df: pl.DataFrame, initial_balance: float = 1000000) -> dict:
    position = 0.0
    trades = []
    cash = initial_balance
    if "buy_signal" not in df.columns or "sell_signal" not in df.columns:
        df = add_signals(df)

    df = df.sort("timestamp") if "timestamp" in df.columns else df
    for idx, row in enumerate(df.iter_rows(named=True)):
        price = row["close"]
        timestamp = row.get("timestamp")
        symbol = row.get("symbol")
        if row["buy_signal"] and position == 0:
            position = cash / price
            cash = 0.0
            trades.append({"idx": idx, "timestamp": timestamp, "symbol": symbol, "side": "BUY", "price": price})
        elif row["sell_signal"] and position > 0:
            cash = position * price
            position = 0.0
            trades.append({"idx": idx, "timestamp": timestamp, "symbol": symbol, "side": "SELL", "price": price})
    final_value = cash + position * df["close"][-1]
    return {
        "initial_balance": initial_balance,
        "final_value": final_value,
        "return_pct": ((final_value / initial_balance) - 1) * 100,
        "trades": trades,
    }


def backtest_by_symbol(df: pl.DataFrame, initial_balance: float = 1000000) -> pl.DataFrame:
    if "symbol" not in df.columns:
        raise ValueError("symbol 컬럼이 필요합니다.")

    results = []
    for symbol_df in df.partition_by("symbol", maintain_order=True):
        symbol = symbol_df["symbol"][0]
        result = backtest_ma_cross(symbol_df, initial_balance=initial_balance)
        results.append(
            {
                "symbol": symbol,
                "initial_balance": result["initial_balance"],
                "final_value": result["final_value"],
                "return_pct": result["return_pct"],
                "trade_count": len(result["trades"]),
            }
        )
    return pl.DataFrame(results).sort("return_pct", descending=True)


def main(
    fpath: str,
    short_ma_period: int = short_ma_period,
    long_ma_period: int = long_ma_period,
    initial_balance: float = 1000000,
):
    df = pl.read_parquet(fpath)
    df = add_signals(df, short_ma_period, long_ma_period)

    if "symbol" in df.columns:
        result_df = backtest_by_symbol(df, initial_balance=initial_balance)
        print(result_df)
    else:
        result = backtest_ma_cross(df, initial_balance=initial_balance)
        print(result)


if __name__ == "__main__":
    import fire
    fire.Fire(main)
