from __future__ import annotations

import polars as pl
import streamlit as st


def render_trade_table(trades: pl.DataFrame) -> None:
    if trades.is_empty():
        st.info("No trades.")
        return
    side_filter = st.multiselect("Side", options=["BUY", "SELL"], default=["BUY", "SELL"])
    filtered = trades.filter(pl.col("side").is_in(side_filter))
    st.dataframe(filtered, width="stretch", hide_index=True)


def render_compare_table(compare: pl.DataFrame) -> None:
    if compare.is_empty():
        st.info("No comparison results.")
        return
    columns = [
        "stock_name",
        "symbol",
        "fast_window",
        "slow_window",
        "total_return",
        "cagr",
        "mdd",
        "sharpe",
        "win_rate",
        "num_trades",
        "final_equity",
    ]
    visible = [column for column in columns if column in compare.columns]
    st.dataframe(compare.select(visible), width="stretch", hide_index=True)
