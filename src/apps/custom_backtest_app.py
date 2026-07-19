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
import importlib
import dashboard.charts

importlib.reload(dashboard.charts)
from dashboard.charts import plot_combined_backtest_chart
from dashboard.components import render_summary
from dashboard.tables import render_compare_table, render_trade_table
from data.market_data import (
    discover_data_roots,
    label_symbol,
    list_available_dates,
    load_ohlcv,
    load_symbol_name_map,
    symbol_lookup_key,
)
from strategies.parameters import StrategyParameter
from strategies.registry import (
    STRATEGY_REGISTRY,
    get_strategy_spec,
    instantiate_strategy,
)


def render_strategy_parameters(
    parameters: tuple[StrategyParameter, ...],
) -> dict[str, int | float | str]:
    values: dict[str, int | float | str] = {}
    for parameter in parameters:
        if parameter.value_type is int:
            values[parameter.key] = int(
                st.number_input(
                    parameter.label,
                    min_value=int(parameter.min_value)
                    if parameter.min_value is not None
                    else None,
                    max_value=int(parameter.max_value)
                    if parameter.max_value is not None
                    else None,
                    value=int(parameter.default),
                    step=int(parameter.step or 1),
                    key=f"strategy_param_{parameter.key}",
                )
            )
        elif parameter.value_type is float:
            values[parameter.key] = float(
                st.number_input(
                    parameter.label,
                    min_value=float(parameter.min_value)
                    if parameter.min_value is not None
                    else None,
                    max_value=float(parameter.max_value)
                    if parameter.max_value is not None
                    else None,
                    value=float(parameter.default),
                    step=float(parameter.step or 0.01),
                    format=parameter.format,
                    key=f"strategy_param_{parameter.key}",
                )
            )
        else:
            values[parameter.key] = st.text_input(
                parameter.label,
                value=str(parameter.default),
                key=f"strategy_param_{parameter.key}",
            )
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
        pl.DataFrame(
            [{"parameter": key, "value": value} for key, value in parameters.items()]
        ),
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

        def format_root(p: Path) -> str:
            try:
                rel = p.relative_to(SRC_DIR.parent)
                return (
                    f"{p.name} (.../{rel.parent})" if str(rel.parent) != "." else p.name
                )
            except ValueError:
                return p.name

        root = st.selectbox(
            "Data Path",
            options=roots,
            format_func=format_root,
            help="Select a directory containing historical parquet data files. The selected full path is shown below.",
        )
        st.caption(f"**Selected path:** `{root}`")

        available_dates = list_available_dates(root)
        if not available_dates:
            st.error("No parquet files found under the selected data path.")
            return

        sample_df = pl.read_parquet(
            sorted(root.glob("*.parquet"))[-1], columns=["symbol"]
        )
        symbols = sorted(
            sample_df.select("symbol").unique().get_column("symbol").to_list()
        )

        def format_symbol_label(val: str) -> str:
            name = symbol_names.get(symbol_lookup_key(val))
            return f"{name} ({val})" if name else str(val)

        symbol = st.selectbox(
            "Symbol", options=symbols, format_func=format_symbol_label
        )

        min_date = available_dates[0]
        max_date = available_dates[-1]
        start_date, end_date = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

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
        initial_balance = st.number_input(
            "Initial Balance", min_value=100_000, value=1_000_000, step=100_000
        )
        fee = st.number_input(
            "Fee",
            min_value=0.0,
            max_value=0.05,
            value=0.0005,
            step=0.0001,
            format="%.5f",
        )
        slippage = st.number_input(
            "Slippage",
            min_value=0.0,
            max_value=0.05,
            value=0.0005,
            step=0.0001,
            format="%.5f",
        )
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
        symbol_names={
            value: label_symbol(value, symbol_names) for value in selected_for_load
        },
        initial_balance=float(initial_balance),
        fee=float(fee),
        slippage=float(slippage),
    )

    # 1. Combined Performance Chart (Full Width at the Top)
    st.subheader("Backtest Performance Analysis")
    combined_fig = plot_combined_backtest_chart(
        ohlcv=result.ohlcv,
        signals=result.signals,
        trades=result.trades,
        equity_curve=result.equity_curve,
    )
    st.plotly_chart(combined_fig, use_container_width=True)

    # 2. Other cells (Columns at the Bottom)
    left_col, right_col = st.columns([0.4, 0.6])

    with left_col:
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
        st.subheader("Compare Results")
        render_compare_table(compare)

    with right_col:
        st.subheader("Trade Log")
        render_trade_table(result.trades)


def main() -> None:
    render_dashboard()


if __name__ == "__main__":
    main()
