from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import streamlit as st

CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = CURRENT_DIR.parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.append(str(SRC_DIR))

from backtest.engine import run_backtest, run_batch_backtests
from dashboard.charts import plot_drawdown, plot_equity_curve, plot_price_with_signals
from dashboard.components import render_summary
from dashboard.tables import render_compare_table, render_trade_table
from data.market_data import (
    discover_data_roots,
    label_symbol,
    list_available_dates,
    load_ohlcv,
    load_symbol_name_map,
)
from strategies.parameters import StrategyParameter
from strategies.registry import STRATEGY_REGISTRY, get_strategy_spec, instantiate_strategy


def render_strategy_parameters(parameters: tuple[StrategyParameter, ...]) -> dict[str, int | float | str]:
    values: dict[str, int | float | str] = {}
    for parameter in parameters:
        if parameter.value_type is int:
            values[parameter.key] = int(
                st.number_input(
                    parameter.label,
                    min_value=int(parameter.min_value) if parameter.min_value is not None else None,
                    max_value=int(parameter.max_value) if parameter.max_value is not None else None,
                    value=int(parameter.default),
                    step=int(parameter.step or 1),
                    key=f"strategy_param_{parameter.key}",
                )
            )
        elif parameter.value_type is float:
            values[parameter.key] = float(
                st.number_input(
                    parameter.label,
                    min_value=float(parameter.min_value) if parameter.min_value is not None else None,
                    max_value=float(parameter.max_value) if parameter.max_value is not None else None,
                    value=float(parameter.default),
                    step=float(parameter.step or 0.01),
                    format=parameter.format,
                    key=f"strategy_param_{parameter.key}",
                )
            )
        else:
            values[parameter.key] = st.text_input(parameter.label, value=str(parameter.default), key=f"strategy_param_{parameter.key}")
    return values


@st.cache_data(show_spinner=False)
def run_backtest_cached(
    df: pl.DataFrame,
    strategy_key: str,
    strategy_params: dict[str, int | float | str],
    symbol: str,
    stock_name: str,
    initial_balance: float,
    fee: float,
    slippage: float,
):
    symbol_df = df.filter(pl.col("symbol") == symbol)
    strategy_spec = get_strategy_spec(strategy_key)
    strategy = instantiate_strategy(strategy_spec, strategy_params)
    return run_backtest(
        df=symbol_df,
        symbol=symbol,
        stock_name=stock_name,
        initial_balance=initial_balance,
        fee=fee,
        slippage=slippage,
        strategy=strategy,
        strategy_name=strategy_spec.name,
    )


@st.cache_data(show_spinner=False)
def run_compare_cached(
    df: pl.DataFrame,
    strategy_key: str,
    strategy_params: dict[str, int | float | str],
    symbols: tuple[str, ...],
    symbol_names: dict[str, str],
    initial_balance: float,
    fee: float,
    slippage: float,
) -> pl.DataFrame:
    strategy_spec = get_strategy_spec(strategy_key)
    strategy = instantiate_strategy(strategy_spec, strategy_params)
    return run_batch_backtests(
        df=df,
        symbols=list(symbols),
        symbol_names=symbol_names,
        initial_balance=initial_balance,
        fee=fee,
        slippage=slippage,
        strategy=strategy,
        strategy_name=strategy_spec.name,
    )


def render_config_summary(
    strategy_name: str,
    parameters: dict[str, int | float | str],
    symbol_label: str,
    start_date: date,
    end_date: date,
    initial_balance: float,
    fee: float,
    slippage: float,
) -> None:
    st.subheader("Config Summary")
    st.markdown(
        "\n".join(
            [
                f"**Strategy:** {strategy_name}",
                f"**Symbol:** {symbol_label}",
                f"**Period:** {start_date.isoformat()} to {end_date.isoformat()}",
                f"**Initial Cash:** {initial_balance:,.0f}",
                f"**Fee:** {fee:.5f}",
                f"**Slippage:** {slippage:.5f}",
            ]
        )
    )
    st.dataframe(
        pl.DataFrame([{"parameter": key, "value": value} for key, value in parameters.items()]),
        use_container_width=True,
        hide_index=True,
    )


def render_dashboard() -> None:
    st.set_page_config(page_title="Backtest Dashboard", layout="wide")
    st.title("Backtest Dashboard")

    roots = discover_data_roots()
    if not roots:
        st.error("No KOSPI 200 1-minute parquet data found.")
        return

    symbol_names = load_symbol_name_map()

    with st.sidebar:
        st.header("Backtest Config")
        root = st.selectbox("Data Path", options=roots, format_func=lambda path: str(path))
        available_dates = list_available_dates(root)
        if not available_dates:
            st.error("No parquet files found under the selected data path.")
            return

        sample_df = pl.read_parquet(sorted(root.glob("*.parquet"))[-1], columns=["symbol"])
        symbols = sorted(sample_df.select("symbol").unique().get_column("symbol").to_list())
        symbol = st.selectbox("Symbol", options=symbols, format_func=lambda value: label_symbol(value, symbol_names))

        min_date = available_dates[0]
        max_date = available_dates[-1]
        start_date, end_date = st.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

        st.header("Strategy")
        strategy_key = st.selectbox(
            "Strategy",
            options=list(STRATEGY_REGISTRY.keys()),
            format_func=lambda key: STRATEGY_REGISTRY[key].name,
        )
        strategy_spec = get_strategy_spec(strategy_key)
        strategy_params = render_strategy_parameters(strategy_spec.parameters)
        compare_symbols = st.multiselect(
            "Compare Symbols",
            options=symbols,
            default=symbols[: min(20, len(symbols))],
            format_func=lambda value: label_symbol(value, symbol_names),
        )

        st.header("Costs")
        initial_balance = st.number_input("Initial Balance", min_value=100_000, value=1_000_000, step=100_000)
        fee = st.number_input("Fee", min_value=0.0, max_value=0.05, value=0.0005, step=0.0001, format="%.5f")
        slippage = st.number_input("Slippage", min_value=0.0, max_value=0.05, value=0.0005, step=0.0001, format="%.5f")
        run_button = st.button("Run Backtest", type="primary")

    if start_date > end_date:
        st.error("Start date must be on or before end date.")
        return

    if not run_button:
        st.info("Configure parameters in the sidebar and run a backtest.")
        return

    selected_for_load = tuple(sorted(set(compare_symbols + [symbol])))
    data = load_ohlcv(str(root), selected_for_load, start_date, end_date)
    if data.is_empty():
        st.warning("No OHLCV rows found for the selected configuration.")
        return

    stock_name = label_symbol(symbol, symbol_names)
    result = run_backtest_cached(
        data,
        strategy_key=strategy_key,
        strategy_params=strategy_params,
        symbol=symbol,
        stock_name=stock_name,
        initial_balance=float(initial_balance),
        fee=float(fee),
        slippage=float(slippage),
    )
    compare = run_compare_cached(
        data,
        strategy_key=strategy_key,
        strategy_params=strategy_params,
        symbols=tuple(compare_symbols or [symbol]),
        symbol_names={value: label_symbol(value, symbol_names) for value in selected_for_load},
        initial_balance=float(initial_balance),
        fee=float(fee),
        slippage=float(slippage),
    )

    top_left, top_right = st.columns(2)
    bottom_left, bottom_right = st.columns(2)

    with top_left:
        render_config_summary(
            strategy_name=strategy_spec.name,
            parameters=strategy_params,
            symbol_label=stock_name,
            start_date=start_date,
            end_date=end_date,
            initial_balance=float(initial_balance),
            fee=float(fee),
            slippage=float(slippage),
        )
        render_summary(result.metrics)

    with top_right:
        st.subheader("Price Chart")
        st.plotly_chart(plot_price_with_signals(result.ohlcv, result.signals, result.trades), use_container_width=True)

    with bottom_left:
        st.subheader("Equity & Drawdown")
        st.plotly_chart(plot_equity_curve(result.equity_curve), use_container_width=True)
        st.plotly_chart(plot_drawdown(result.equity_curve), use_container_width=True)

    with bottom_right:
        st.subheader("Trade Log")
        render_trade_table(result.trades)
        st.subheader("Compare")
        render_compare_table(compare)


def main() -> None:
    render_dashboard()


if __name__ == "__main__":
    main()
