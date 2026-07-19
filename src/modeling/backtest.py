"""
백테스팅 모듈 (Backtesting Module)

tmp.py에서 계산된 기술적 지표와 시그널을 바탕으로
투자 수익률, 승률, MDD 등의 성과를 분석합니다.
"""

import polars as pl
import yfinance as yf
from src.modeling.tmp import add_rsi, add_macd, add_squeeze, add_signals


def run_backtest(df: pl.DataFrame, initial_capital: float = 10000.0) -> pl.DataFrame:
    """
    간이 백테스팅 실행 (Vectorized Approach)

    매매 규칙:
    - Long: long_signal == True 시 진입 -> momentum < 0 시 청산
    - Short: short_signal == True 시 진입 -> momentum > 0 시 청산
    """

    # 1. 포지션 상태 계산 (이해를 돕기 위해 반복문 대신 논리 연산 사용 가능하지만,
    # 정교한 상태 관리를 위해 iterate가 유리할 수 있음. 여기서는 단순화된 벡터 방식 시도)

    # 포지션 태그 생성
    # 1: Long, -1: Short, 0: None

    df = df.with_columns(
        [
            pl.lit(0).alias("position"),
            pl.col("Close").pct_change().fill_null(0).alias("returns"),
        ]
    )

    # 실제 백테스트 로직 (Iterative for complex logic)
    # Polars DataFrame을 dict list로 변환하여 순회 (속도와 유연성 절충)
    data = df.to_dicts()
    current_pos = 0  # 0: None, 1: Long, -1: Short

    for i in range(1, len(data)):
        # Exit Logic
        if current_pos == 1:
            if data[i]["momentum"] < 0:  # 모멘텀 꺾이면 매도
                current_pos = 0
        elif current_pos == -1:
            if data[i]["momentum"] > 0:  # 모멘텀 꺾이면 매수
                current_pos = 0

        # Entry Logic (Only if not in position)
        if current_pos == 0:
            if data[i]["long_signal"]:
                current_pos = 1
            elif data[i]["short_signal"]:
                current_pos = -1

        data[i]["position"] = current_pos

    # 다시 DataFrame으로 변환
    res_df = pl.from_dicts(data)

    # 수익률 계산 (포지션을 잡은 다음 봉의 수익률을 가져감)
    res_df = res_df.with_columns(
        [
            (pl.col("position").shift(1).fill_null(0) * pl.col("returns")).alias(
                "strategy_returns"
            )
        ]
    )

    # 누적 수익률
    res_df = res_df.with_columns(
        [(1 + pl.col("strategy_returns")).cum_prod().alias("cumulative_returns")]
    )

    return res_df


def print_performance(df: pl.DataFrame, initial_capital: float = 10000.0):
    """
    백테스팅 성과 지표 출력
    """
    final_return = df["cumulative_returns"].tail(1)[0]
    total_return = (final_return - 1) * 100

    # MDD (Maximum Drawdown)
    df = df.with_columns([pl.col("cumulative_returns").cum_max().alias("cum_max")])
    df = df.with_columns(
        [(pl.col("cumulative_returns") / pl.col("cum_max") - 1).alias("drawdown")]
    )
    mdd = df["drawdown"].min() * 100

    # Win Rate (매매 횟수 기준은 아니지만, 양의 수익을 기록한 봉의 비율로 간략화)
    # 실제로는 매매 건별 승률을 구하는 것이 좋음
    positive_trades = df.filter(pl.col("strategy_returns") > 0).height
    negative_trades = df.filter(pl.col("strategy_returns") < 0).height
    win_rate = (
        (positive_trades / (positive_trades + negative_trades) * 100)
        if (positive_trades + negative_trades) > 0
        else 0
    )

    print("-" * 30)
    print("      BACKTEST RESULTS      ")
    print("-" * 30)
    print(f"Total Return:    {total_return:.2f}%")
    print(f"Max Drawdown:    {mdd:.2f}%")
    print(f"Win Rate (bars): {win_rate:.2f}%")
    print(f"Final Value:     ${initial_capital * final_return:.2f}")
    print("-" * 30)


if __name__ == "__main__":
    # 1. 데이터 로드
    symbol = "QQQ"
    print(f"Downloading data for {symbol}...")
    df_raw = yf.download(symbol, interval="1m", period="7d", group_by="column")
    df_raw = df_raw.dropna()

    if hasattr(df_raw.columns, "levels"):
        df_raw.columns = [col[0] for col in df_raw.columns]

    df = pl.from_pandas(df_raw)

    # 2. 지표 및 시그널 계산
    print("Calculating indicators and signals...")
    df = add_rsi(df)
    df = add_macd(df)
    df = add_squeeze(df)
    df = add_signals(df)

    # 3. 백테스트 실행
    print("Running backtest...")
    results = run_backtest(df)

    # 4. 결과 출력
    print_performance(results)
