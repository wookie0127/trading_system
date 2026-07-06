from __future__ import annotations

import streamlit as st


def render_summary(metrics: dict) -> None:
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    col1.metric("Total Return", f"{metrics.get('total_return', 0.0):.2%}")
    col2.metric("CAGR", f"{metrics.get('cagr', 0.0):.2%}")
    col3.metric("MDD", f"{metrics.get('mdd', 0.0):.2%}")
    col4.metric("Sharpe", f"{metrics.get('sharpe', 0.0):.2f}")
    col5.metric("Win Rate", f"{metrics.get('win_rate', 0.0):.2%}")
    col6.metric("Trades", f"{metrics.get('num_trades', 0)}")
