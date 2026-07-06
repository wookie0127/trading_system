from __future__ import annotations

import polars as pl
import plotly.graph_objects as go


def plot_price_with_signals(ohlcv: pl.DataFrame, signals: pl.DataFrame, trades: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Candlestick(
            x=ohlcv.get_column("timestamp"),
            open=ohlcv.get_column("open"),
            high=ohlcv.get_column("high"),
            low=ohlcv.get_column("low"),
            close=ohlcv.get_column("close"),
            name="Price",
        )
    )

    if "ma_fast" in ohlcv.columns:
        fig.add_trace(go.Scatter(x=ohlcv.get_column("timestamp"), y=ohlcv.get_column("ma_fast"), mode="lines", name="Fast MA"))
    if "ma_slow" in ohlcv.columns:
        fig.add_trace(go.Scatter(x=ohlcv.get_column("timestamp"), y=ohlcv.get_column("ma_slow"), mode="lines", name="Slow MA"))

    if not trades.is_empty():
        buy_trades = trades.filter(pl.col("side") == "BUY")
        sell_trades = trades.filter(pl.col("side") == "SELL")
        fig.add_trace(
            go.Scatter(
                x=buy_trades.get_column("timestamp") if not buy_trades.is_empty() else [],
                y=buy_trades.get_column("price") if not buy_trades.is_empty() else [],
                mode="markers",
                name="Buy",
                marker=dict(symbol="triangle-up", size=11, color="#059669"),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=sell_trades.get_column("timestamp") if not sell_trades.is_empty() else [],
                y=sell_trades.get_column("price") if not sell_trades.is_empty() else [],
                mode="markers",
                name="Sell",
                marker=dict(symbol="triangle-down", size=11, color="#dc2626"),
            )
        )

    fig.update_layout(height=650, xaxis_rangeslider_visible=False, hovermode="x unified", template="plotly_white")
    return fig


def plot_equity_curve(equity_curve: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=equity_curve.get_column("timestamp"), y=equity_curve.get_column("equity"), mode="lines", name="Equity"))
    fig.update_layout(height=350, hovermode="x unified", template="plotly_white")
    return fig


def plot_drawdown(equity_curve: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=equity_curve.get_column("timestamp"),
            y=equity_curve.get_column("drawdown"),
            mode="lines",
            fill="tozeroy",
            name="Drawdown",
            line=dict(color="#dc2626"),
        )
    )
    fig.update_layout(height=300, hovermode="x unified", template="plotly_white")
    return fig
