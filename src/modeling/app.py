import streamlit as st
import polars as pl
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from src.modeling.tmp import add_rsi, add_macd, add_squeeze, add_signals
from src.modeling.backtest import run_backtest

st.set_page_config(layout="wide", page_title="Trading Backtest Dashboard")

st.title("📈 Squeeze Momentum Backtest Dashboard")

# side bar - 설정
st.sidebar.header("Settings")
symbol = st.sidebar.text_input("Symbol", value="QQQ")
period = st.sidebar.selectbox("Period", ["1mo", "3mo", "6mo", "1y", "max"], index=0)
interval = st.sidebar.selectbox("Interval", ["1m", "5m", "15m", "60m", "1d"], index=1)

st.sidebar.info("Note: 1m interval only supports up to 7 days of data.")

# 1일씩 추가 기능용 세션 상태
if "days_to_show" not in st.session_state:
    st.session_state.days_to_show = 1


def reset_incremental():
    st.session_state.days_to_show = 1


st.sidebar.button("Reset Simulation", on_click=reset_incremental)


# 데이터 로드 (캐싱)
@st.cache_data
def load_data(symbol, period, interval):
    # 1m일 경우 기간 제한 (에러 방지)
    if interval == "1m" and period not in ["1d", "5d", "7d"]:
        period = "7d"
        st.warning("1m interval is limited to 7d. Adjusting period to 7d.")

    df_raw = yf.download(symbol, period=period, interval=interval, group_by="column")
    if hasattr(df_raw.columns, "levels"):
        df_raw.columns = [col[0] for col in df_raw.columns]
    df_raw = df_raw.dropna()
    return pl.from_pandas(df_raw.reset_index())


df_full = load_data(symbol, period, interval)

if df_full.height > 0:
    # 1일씩 추가 기능 구현
    total_len = df_full.height

    # 간격별 하루치 데이터 분량(step) 설정
    if interval == "1d":
        step = 1
    elif interval == "1m":
        step = 390  # 미국 시장 6.5시간 기준 390분
    elif interval == "5m":
        step = 78  # 390 / 5
    elif interval == "15m":
        step = 26  # 390 / 15
    else:
        step = 6  # 1시간봉 기준

    col1, col2 = st.sidebar.columns(2)
    if col1.button("Next Step ➡️"):
        st.session_state.days_to_show = min(
            st.session_state.days_to_show + step, total_len
        )

    if col2.button("Show ALL"):
        st.session_state.days_to_show = total_len

    # 현재까지의 데이터 슬라이싱
    df_current = df_full.head(st.session_state.days_to_show)

    # 지표 계산
    df_current = add_rsi(df_current)
    df_current = add_macd(df_current)
    df_current = add_squeeze(df_current)
    df_current = add_signals(df_current)

    # 백테스트 실행
    results = run_backtest(df_current)

    # --- 대시보드 출력 ---

    # 1. 성과 요약 카드
    final_val = results["cumulative_returns"].tail(1)[0]
    total_ret = (final_val - 1) * 100

    m1, m2, m3 = st.columns(3)
    m1.metric("Total Return", f"{total_ret:.2f}%")
    m2.metric("Final Value (vs $10k)", f"${10000 * final_val:.2f}")
    m3.metric("Data Points", f"{st.session_state.days_to_show} / {total_len}")

    # 2. 메인 차트 (Price + Signals)
    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.5, 0.25, 0.25],
    )

    # Price & Position
    fig.add_trace(
        go.Scatter(
            x=results["Datetime"] if "Datetime" in results.columns else results["Date"],
            y=results["Close"],
            name="Price",
            line=dict(color="white"),
        ),
        row=1,
        col=1,
    )

    # Long Signals
    longs = results.filter(pl.col("long_signal") == True)
    if longs.height > 0:
        fig.add_trace(
            go.Scatter(
                x=longs["Datetime"] if "Datetime" in longs.columns else longs["Date"],
                y=longs["Close"],
                mode="markers",
                name="Long Entry",
                marker=dict(symbol="triangle-up", size=10, color="lime"),
            ),
            row=1,
            col=1,
        )

    # Short Signals
    shorts = results.filter(pl.col("short_signal") == True)
    if shorts.height > 0:
        fig.add_trace(
            go.Scatter(
                x=shorts["Datetime"]
                if "Datetime" in shorts.columns
                else shorts["Date"],
                y=shorts["Close"],
                mode="markers",
                name="Short Entry",
                marker=dict(symbol="triangle-down", size=10, color="red"),
            ),
            row=1,
            col=1,
        )

    # MACD
    fig.add_trace(
        go.Bar(
            x=results["Datetime"] if "Datetime" in results.columns else results["Date"],
            y=results["macd_hist"],
            name="MACD Hist",
        ),
        row=2,
        col=1,
    )

    # Squeeze Momentum
    # marker_color 계산 시 return_dtype을 명시하거나 pl.when 사용 (에러 방지)
    momentum_colors = results.with_columns(
        pl.when(pl.col("momentum") > 0)
        .then(pl.lit("cyan"))
        .otherwise(pl.lit("orange"))
        .alias("m_color")
    )["m_color"]

    fig.add_trace(
        go.Bar(
            x=results["Datetime"] if "Datetime" in results.columns else results["Date"],
            y=results["momentum"],
            name="Squeeze Momentum",
            marker_color=momentum_colors,
        ),
        row=3,
        col=1,
    )

    fig.update_layout(height=800, template="plotly_dark", showlegend=True)
    st.plotly_chart(fig, use_container_width=True)

    # 3. 누적 수익률 차트
    st.subheader("Performance Strategy vs Buy & Hold")
    fig_ret = go.Figure()
    fig_ret.add_trace(
        go.Scatter(
            x=results["Datetime"] if "Datetime" in results.columns else results["Date"],
            y=results["cumulative_returns"],
            name="Strategy",
            line=dict(color="lime"),
        )
    )

    # Buy & Hold 비교용
    bh_ret = results["Close"] / results["Close"][0]
    fig_ret.add_trace(
        go.Scatter(
            x=results["Datetime"] if "Datetime" in results.columns else results["Date"],
            y=bh_ret,
            name="Buy & Hold",
            line=dict(color="gray", dash="dash"),
        )
    )

    fig_ret.update_layout(height=400, template="plotly_dark")
    st.plotly_chart(fig_ret, use_container_width=True)

else:
    st.error("No data found for the given symbol and period.")
