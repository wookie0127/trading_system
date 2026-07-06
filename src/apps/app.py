from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from data.market_data import (
    discover_data_roots,
    list_available_dates,
    load_intraday_window,
    summarize_period_returns,
)
from strategies.ma_goldencross import add_signals, backtest_ma_cross


def _symbol_frame(frame: pl.DataFrame, symbol: str) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    if "symbol" not in frame.columns:
        raise ValueError("Data frame does not contain a symbol column")
    return frame.filter(pl.col("symbol") == symbol).sort("timestamp")


def build_symbol_figure(frame: pl.DataFrame, short_ma: int, long_ma: int) -> go.Figure:
    signal_frame = add_signals(frame, short_ma_period=short_ma, long_ma_period=long_ma)

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.05,
    )

    fig.add_trace(
        go.Scatter(
            x=signal_frame["timestamp"],
            y=signal_frame["close"],
            name="Close",
            line=dict(color="#1f77b4", width=1.6),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=signal_frame["timestamp"],
            y=signal_frame["short_ma"],
            name=f"MA {short_ma}",
            line=dict(color="#ff7f0e", width=1.4),
        ),
        row=1,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=signal_frame["timestamp"],
            y=signal_frame["long_ma"],
            name=f"MA {long_ma}",
            line=dict(color="#2ca02c", width=1.4),
        ),
        row=1,
        col=1,
    )

    buy_points = signal_frame.filter(pl.col("buy_signal") == True)  # noqa: E712
    sell_points = signal_frame.filter(pl.col("sell_signal") == True)  # noqa: E712

    if not buy_points.is_empty():
        fig.add_trace(
            go.Scatter(
                x=buy_points["timestamp"],
                y=buy_points["close"],
                mode="markers",
                name="Buy Signal",
                marker=dict(symbol="triangle-up", size=10, color="#00cc96"),
            ),
            row=1,
            col=1,
        )
    if not sell_points.is_empty():
        fig.add_trace(
            go.Scatter(
                x=sell_points["timestamp"],
                y=sell_points["close"],
                mode="markers",
                name="Sell Signal",
                marker=dict(symbol="triangle-down", size=10, color="#ef553b"),
            ),
            row=1,
            col=1,
        )

    fig.add_trace(
        go.Bar(
            x=signal_frame["timestamp"],
            y=signal_frame["volume"],
            name="Volume",
            marker_color="#8c8c8c",
            opacity=0.6,
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        height=780,
        template="plotly_white",
        legend_orientation="h",
        legend_y=1.02,
        margin=dict(l=30, r=20, t=40, b=30),
    )
    return fig


def render_dashboard() -> None:
    st.set_page_config(page_title="MA Golden Cross Explorer", layout="wide")
    st.title("MA Golden Cross Explorer")
    st.caption("KOSPI 200 1분봉 기준 종목별 수익률과 기간별 MA cross 결과를 탐색합니다.")

    roots = discover_data_roots()
    if not roots:
        st.error("No KOSPI 200 1-minute parquet data found.")
        return

    with st.sidebar:
        st.header("Filters")
        root = st.selectbox("Data root", options=roots, format_func=lambda p: str(p))
        available_dates = list_available_dates(root)
        if not available_dates:
            st.error("No parquet files found under the selected data root.")
            return

        min_date = available_dates[0]
        max_date = available_dates[-1]
        start_date, end_date = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )
        short_ma = st.number_input("Short MA", min_value=2, max_value=240, value=20, step=1)
        long_ma = st.number_input("Long MA", min_value=3, max_value=480, value=60, step=1)
        initial_balance = st.number_input("Initial balance", min_value=100_000, value=1_000_000, step=100_000)

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    window = load_intraday_window(str(root), start_date, end_date)
    if window.frame.is_empty():
        st.warning("No rows found for the selected date range.")
        return

    returns = summarize_period_returns(window.frame)
    symbol_options = returns.get_column("symbol").to_list()
    default_symbol = symbol_options[0] if symbol_options else None

    summary_cols = st.columns(4)
    summary_cols[0].metric("Files", len(window.files))
    summary_cols[1].metric("Rows", f"{window.frame.height:,}")
    summary_cols[2].metric("Symbols", len(symbol_options))
    summary_cols[3].metric("Date span", f"{start_date.isoformat()} to {end_date.isoformat()}")

    left, right = st.columns([0.36, 0.64])

    with left:
        st.subheader("Period Returns")
        st.dataframe(
            returns.select(
                [
                    "symbol",
                    "return_pct",
                    "return_multiple",
                    "start_close",
                    "end_close",
                    "bars",
                    "start_ts",
                    "end_ts",
                ]
            ),
            use_container_width=True,
            hide_index=True,
        )

        if not returns.is_empty():
            top = returns.head(10).select(["symbol", "return_pct", "bars"])
            st.markdown("Top 10")
            st.dataframe(top, use_container_width=True, hide_index=True)

    with right:
        st.subheader("Symbol Detail")
        selected_symbol = st.selectbox("Symbol", options=symbol_options, index=0 if default_symbol else None)

        symbol_frame = _symbol_frame(window.frame, selected_symbol)
        if symbol_frame.is_empty():
            st.warning("Selected symbol has no data in the chosen period.")
            return

        symbol_returns = returns.filter(pl.col("symbol") == selected_symbol).to_dicts()[0]
        backtest_result = backtest_ma_cross(
            add_signals(symbol_frame, short_ma_period=int(short_ma), long_ma_period=int(long_ma)),
            initial_balance=float(initial_balance),
        )

        metric_cols = st.columns(3)
        metric_cols[0].metric("Period return", f"{symbol_returns['return_pct']:.2f}%")
        metric_cols[1].metric("Final value", f"{backtest_result['final_value']:,.0f}")
        metric_cols[2].metric("Trades", len(backtest_result["trades"]))

        st.plotly_chart(
            build_symbol_figure(symbol_frame, short_ma=int(short_ma), long_ma=int(long_ma)),
            use_container_width=True,
        )

        trades_df = (
            pl.DataFrame(
                backtest_result["trades"],
                orient="row",
                schema=["bar_index", "side", "price"],
            )
            if backtest_result["trades"]
            else pl.DataFrame(schema=["bar_index", "side", "price"])
        )
        st.subheader("Trades")
        st.dataframe(trades_df, use_container_width=True, hide_index=True)


def main() -> None:
    render_dashboard()


if __name__ == "__main__":
    main()
