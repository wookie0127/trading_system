from __future__ import annotations

import polars as pl
import plotly.graph_objects as go


def plot_price_with_signals(
    ohlcv: pl.DataFrame, signals: pl.DataFrame, trades: pl.DataFrame
) -> go.Figure:
    fig = go.Figure()

    # 1. Price Line (Slate Gray)
    fig.add_trace(
        go.Scatter(
            x=ohlcv.get_column("timestamp"),
            y=ohlcv.get_column("close"),
            name="Price (Close)",
            line=dict(color="#2c3e50", width=1.5),
        )
    )

    # 2. Moving Averages with TradingView colors
    if "ma_fast" in ohlcv.columns:
        fig.add_trace(
            go.Scatter(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("ma_fast"),
                mode="lines",
                name="Fast MA",
                line=dict(color="#2962ff", width=1.2),
            )
        )
    if "ma_slow" in ohlcv.columns:
        fig.add_trace(
            go.Scatter(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("ma_slow"),
                mode="lines",
                name="Slow MA",
                line=dict(color="#ff9800", width=1.2),
            )
        )

    # 3. TradingView-style Volume Overlay at the bottom (secondary y-axis)
    if "volume" in ohlcv.columns and not ohlcv.is_empty():
        # Green bar if close >= open, else red bar
        vol_colors = [
            "#00e676" if c >= o else "#ff1744"
            for c, o in zip(
                ohlcv.get_column("close").to_list(), ohlcv.get_column("open").to_list()
            )
        ]
        fig.add_trace(
            go.Bar(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("volume"),
                name="Volume",
                marker_color=vol_colors,
                opacity=0.2,
                yaxis="y2",
            )
        )

    # 4. Small Trade Markers with White Outline (TradingView style)
    if not trades.is_empty():
        buy_trades = trades.filter(pl.col("side") == "BUY")
        sell_trades = trades.filter(pl.col("side") == "SELL")
        fig.add_trace(
            go.Scatter(
                x=buy_trades.get_column("timestamp")
                if not buy_trades.is_empty()
                else [],
                y=buy_trades.get_column("price") if not buy_trades.is_empty() else [],
                mode="markers",
                name="Buy",
                marker=dict(
                    symbol="triangle-up",
                    size=8,
                    color="#00e676",
                    line=dict(width=1.5, color="#ffffff"),
                ),
            )
        )
        fig.add_trace(
            go.Scatter(
                x=sell_trades.get_column("timestamp")
                if not sell_trades.is_empty()
                else [],
                y=sell_trades.get_column("price") if not sell_trades.is_empty() else [],
                mode="markers",
                name="Sell",
                marker=dict(
                    symbol="triangle-down",
                    size=8,
                    color="#ff1744",
                    line=dict(width=1.5, color="#ffffff"),
                ),
            )
        )

    # Calculate ranges and configure axes
    max_vol = (
        ohlcv.get_column("volume").max()
        if not ohlcv.is_empty() and "volume" in ohlcv.columns
        else 1
    )

    fig.update_layout(
        height=650,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        font=dict(family="'Trebuchet MS', 'Inter', sans-serif", size=11),
        margin=dict(l=50, r=50, t=30, b=30),
        xaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            linewidth=1,
            mirror=True,
            showline=True,
        ),
        yaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            linewidth=1,
            mirror=True,
            showline=True,
            side="left",
        ),
        yaxis2=dict(
            overlaying="y",
            side="right",
            showgrid=False,
            showticklabels=False,
            range=[0, max_vol * 4],  # Volume bars take at most 25% of visual height
        ),
        hoverlabel=dict(
            bgcolor="#131722",
            font_size=12,
            font_family="'Trebuchet MS', 'Inter', sans-serif",
            font_color="#ffffff",
        ),
    )
    return fig


def plot_equity_curve(equity_curve: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=equity_curve.get_column("timestamp"),
            y=equity_curve.get_column("equity"),
            mode="lines",
            name="Equity",
            line=dict(color="#089981", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(8, 153, 129, 0.05)",
        )
    )
    fig.update_layout(
        height=350,
        hovermode="x unified",
        font=dict(family="'Trebuchet MS', 'Inter', sans-serif", size=11),
        xaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            showline=True,
        ),
        yaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            showline=True,
            title="Equity",
        ),
    )
    return fig


def plot_drawdown(equity_curve: pl.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=equity_curve.get_column("timestamp"),
            y=equity_curve.get_column("drawdown") * 100,  # percentage
            mode="lines",
            fill="tozeroy",
            fillcolor="rgba(242, 54, 69, 0.05)",
            name="Drawdown (%)",
            line=dict(color="#f23645", width=1.2),
        )
    )
    fig.update_layout(
        height=300,
        hovermode="x unified",
        font=dict(family="'Trebuchet MS', 'Inter', sans-serif", size=11),
        xaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            showline=True,
        ),
        yaxis=dict(
            gridcolor="rgba(128, 128, 128, 0.1)",
            linecolor="rgba(128, 128, 128, 0.2)",
            showline=True,
            title="Drawdown (%)",
        ),
    )
    return fig


def plot_combined_backtest_chart(
    ohlcv: pl.DataFrame,
    signals: pl.DataFrame,
    trades: pl.DataFrame,
    equity_curve: pl.DataFrame,
) -> go.Figure:
    from plotly.subplots import make_subplots

    # Specifying secondary_y=True only for Row 1 (Price/Volume overlay)
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        row_heights=[0.5, 0.25, 0.25],
        specs=[
            [{"secondary_y": True}],
            [{"secondary_y": False}],
            [{"secondary_y": False}],
        ],
    )

    # --- ROW 1: Price, MAs, Signals, Volume ---
    # Price
    fig.add_trace(
        go.Scatter(
            x=ohlcv.get_column("timestamp"),
            y=ohlcv.get_column("close"),
            name="Price",
            line=dict(color="#2c3e50", width=1.5),
        ),
        row=1,
        col=1,
        secondary_y=False,
    )

    # Fast MA
    if "ma_fast" in ohlcv.columns:
        fig.add_trace(
            go.Scatter(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("ma_fast"),
                mode="lines",
                name="Fast MA",
                line=dict(color="#2962ff", width=1.2),
            ),
            row=1,
            col=1,
            secondary_y=False,
        )

    # Slow MA
    if "ma_slow" in ohlcv.columns:
        fig.add_trace(
            go.Scatter(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("ma_slow"),
                mode="lines",
                name="Slow MA",
                line=dict(color="#ff9800", width=1.2),
            ),
            row=1,
            col=1,
            secondary_y=False,
        )

    # Volume (Secondary Y-Axis on Row 1)
    if "volume" in ohlcv.columns and not ohlcv.is_empty():
        vol_colors = [
            "#00e676" if c >= o else "#ff1744"
            for c, o in zip(
                ohlcv.get_column("close").to_list(), ohlcv.get_column("open").to_list()
            )
        ]
        fig.add_trace(
            go.Bar(
                x=ohlcv.get_column("timestamp"),
                y=ohlcv.get_column("volume"),
                name="Volume",
                marker_color=vol_colors,
                opacity=0.15,
            ),
            row=1,
            col=1,
            secondary_y=True,
        )

    # Buy / Sell Markers
    if not trades.is_empty():
        buy_trades = trades.filter(pl.col("side") == "BUY")
        sell_trades = trades.filter(pl.col("side") == "SELL")
        if not buy_trades.is_empty():
            fig.add_trace(
                go.Scatter(
                    x=buy_trades.get_column("timestamp"),
                    y=buy_trades.get_column("price"),
                    mode="markers",
                    name="Buy",
                    marker=dict(
                        symbol="triangle-up",
                        size=8,
                        color="#00e676",
                        line=dict(width=1.5, color="#ffffff"),
                    ),
                ),
                row=1,
                col=1,
                secondary_y=False,
            )
        if not sell_trades.is_empty():
            fig.add_trace(
                go.Scatter(
                    x=sell_trades.get_column("timestamp"),
                    y=sell_trades.get_column("price"),
                    mode="markers",
                    name="Sell",
                    marker=dict(
                        symbol="triangle-down",
                        size=8,
                        color="#ff1744",
                        line=dict(width=1.5, color="#ffffff"),
                    ),
                ),
                row=1,
                col=1,
                secondary_y=False,
            )

    # --- ROW 2: Equity Curve ---
    fig.add_trace(
        go.Scatter(
            x=equity_curve.get_column("timestamp"),
            y=equity_curve.get_column("equity"),
            mode="lines",
            name="Equity",
            line=dict(color="#089981", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(8, 153, 129, 0.05)",
        ),
        row=2,
        col=1,
    )

    # --- ROW 3: Drawdown ---
    fig.add_trace(
        go.Scatter(
            x=equity_curve.get_column("timestamp"),
            y=equity_curve.get_column("drawdown") * 100,  # percentage
            mode="lines",
            name="Drawdown (%)",
            line=dict(color="#f23645", width=1.2),
            fill="tozeroy",
            fillcolor="rgba(242, 54, 69, 0.05)",
        ),
        row=3,
        col=1,
    )

    # --- Layout & Subplots styling ---
    max_vol = (
        ohlcv.get_column("volume").max()
        if not ohlcv.is_empty() and "volume" in ohlcv.columns
        else 1
    )

    fig.update_layout(
        height=850,
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
        template="plotly_white",
        font=dict(
            family="'Trebuchet MS', 'Inter', sans-serif", size=11, color="#373d49"
        ),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        margin=dict(l=50, r=50, t=30, b=30),
        # Row 1 Axis
        xaxis=dict(gridcolor="#f0f3fa", linecolor="#e0e3eb", showline=True),
        yaxis=dict(
            gridcolor="#f0f3fa", linecolor="#e0e3eb", showline=True, side="left"
        ),
        yaxis2=dict(
            showgrid=False,
            showticklabels=False,
            range=[0, max_vol * 4],  # 25% height for Volume overlay
        ),
        # Row 2 Axis
        xaxis2=dict(gridcolor="#f0f3fa", linecolor="#e0e3eb", showline=True),
        yaxis3=dict(
            gridcolor="#f0f3fa", linecolor="#e0e3eb", showline=True, title="Equity"
        ),
        # Row 3 Axis
        xaxis3=dict(gridcolor="#f0f3fa", linecolor="#e0e3eb", showline=True),
        yaxis4=dict(
            gridcolor="#f0f3fa",
            linecolor="#e0e3eb",
            showline=True,
            title="Drawdown (%)",
        ),
        hoverlabel=dict(
            bgcolor="#131722",
            font_size=12,
            font_family="'Trebuchet MS', 'Inter', sans-serif",
            font_color="#ffffff",
        ),
    )
    return fig
