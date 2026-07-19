from __future__ import annotations

import polars as pl


def build_feature_dataset(raw_data: pl.DataFrame) -> pl.DataFrame:
    if raw_data.is_empty():
        return raw_data.clone()

    features = (
        raw_data.sort(["symbol", "date"])
        .with_columns(
            pl.col("close").pct_change().over("symbol").alias("return_1d"),
            pl.col("close").pct_change(5).over("symbol").alias("return_5d"),
            pl.col("close").pct_change(20).over("symbol").alias("return_20d"),
            pl.col("close").rolling_mean(5).over("symbol").alias("ma_5"),
            pl.col("close").rolling_mean(20).over("symbol").alias("ma_20"),
            pl.col("close").rolling_mean(60).over("symbol").alias("ma_60"),
            pl.col("close").diff().over("symbol").alias("_delta"),
        )
        .with_columns(
            pl.col("return_1d").rolling_std(20).over("symbol").alias("volatility_20d"),
            pl.when(pl.col("_delta") > 0)
            .then(pl.col("_delta"))
            .otherwise(0.0)
            .alias("_gain"),
            pl.when(pl.col("_delta") < 0)
            .then(-pl.col("_delta"))
            .otherwise(0.0)
            .alias("_loss"),
        )
        .with_columns(
            pl.col("_gain").rolling_mean(14).over("symbol").alias("_avg_gain"),
            pl.col("_loss").rolling_mean(14).over("symbol").alias("_avg_loss"),
        )
        .with_columns(
            (
                100
                - (
                    100
                    / (1 + (pl.col("_avg_gain") / pl.col("_avg_loss").replace(0, None)))
                )
            ).alias("rsi_14")
        )
        .drop(["_delta", "_gain", "_loss", "_avg_gain", "_avg_loss"])
    )

    context = (
        features.select(["date", "symbol", "return_1d"])
        .filter(pl.col("symbol").is_in(["vix", "usdkrw", "btc"]))
        .pivot(
            index="date", on="symbol", values="return_1d", aggregate_function="first"
        )
        .rename(
            {
                "vix": "vix_change_1d",
                "usdkrw": "usdkrw_change_1d",
                "btc": "btc_return_1d",
            }
        )
    )

    for col in ["vix_change_1d", "usdkrw_change_1d", "btc_return_1d"]:
        if col not in context.columns:
            context = context.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    return features.join(
        context.select(["date", "vix_change_1d", "usdkrw_change_1d", "btc_return_1d"]),
        on="date",
        how="left",
    ).sort(["date", "symbol"])
