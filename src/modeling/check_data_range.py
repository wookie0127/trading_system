import yfinance as yf
import pandas as pd
import argparse
import re
from datetime import datetime, timedelta

# Yahoo Finance 데이터 제한 정의 (일 단위)
# max_request: 한 번의 API 호출로 가져올 수 있는 최대 기간
# max_lookback: 현재 시점으로부터 거슬러 올라갈 수 있는 최대 기간
YF_LIMITS = {
    "1m":  {"max_request": 7,   "max_lookback": 30},
    "2m":  {"max_request": 60,  "max_lookback": 60},
    "5m":  {"max_request": 60,  "max_lookback": 60},
    "15m": {"max_request": 60,  "max_lookback": 60},
    "30m": {"max_request": 60,  "max_lookback": 60},
    "90m": {"max_request": 60,  "max_lookback": 60},
    "60m": {"max_request": 730, "max_lookback": 730},
    "1h":  {"max_request": 730, "max_lookback": 730},
    "1d":  {"max_request": None, "max_lookback": None}, # 일봉은 사실상 제한 없음
}

def parse_period_to_days(period_str: str) -> int:
    """
    yfinance의 period 문자열(예: '3mo', '1y', '7d')을 대략적인 일 단위 숫자로 변환합니다.
    """
    if period_str == "max":
        return 999999  # 아주 큰 값
    
    match = re.match(r"(\d+)([a-z]+)", period_str.lower())
    if not match:
        raise ValueError(f"Invalid period format: {period_str}")
    
    value, unit = match.groups()
    value = int(value)
    
    if unit in ["d", "day"]:
        return value
    elif unit in ["mo", "month"]:
        return value * 30
    elif unit in ["y", "year"]:
        return value * 365
    elif unit == "ytd":
        # 현재 연도 시작일부터 오늘까지
        now = datetime.now()
        start_of_year = datetime(now.year, 1, 1)
        return (now - start_of_year).days
    else:
        raise ValueError(f"Unknown period unit: {unit}")

def validate_yf_params(symbol: str, interval: str, period: str):
    """
    Yahoo Finance 요청 파라미터가 유효한지 검증하고 제한을 넘으면 ValueError를 발생시킵니다.
    """
    if interval not in YF_LIMITS:
        # 정의되지 않은 interval은 기본적으로 허용하되 경고 없이 진행 (또는 필요시 추가)
        return
    
    limit = YF_LIMITS[interval]
    requested_days = parse_period_to_days(period)
    
    # 1. 한 번의 요청(max_request) 제한 체크
    if limit["max_request"] is not None and requested_days > limit["max_request"]:
        raise ValueError(
            f"[{symbol}] Interval '{interval}'은(는) 한 번의 요청으로 최대 {limit['max_request']}일치만 가져올 수 있습니다. "
            f"요청된 기간: {period} ({requested_days}일)"
        )
    
    # 2. 전체 Lookback 제한 체크
    if limit["max_lookback"] is not None and requested_days > limit["max_lookback"]:
        raise ValueError(
            f"[{symbol}] Interval '{interval}'은(는) 최근 {limit['max_lookback']}일 이내의 데이터만 조회 가능합니다. "
            f"요청된 기간: {period} ({requested_days}일)"
        )

def check_data_range(symbol: str, interval: str, period: str):
    """
    검증 후 데이터를 다운로드하여 범위를 출력합니다.
    """
    print(f"Checking data for {symbol} (Interval: {interval}, Period: {period})...")
    
    # 파라미터 검증 (제한 위반 시 raise)
    validate_yf_params(symbol, interval, period)
    
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False)
        if df.empty:
            print("  FAILED: No data returned from Yahoo Finance.")
            return
        
        # MultiIndex 처리
        if hasattr(df.columns, "levels"):
            df.columns = [col[0] for col in df.columns]
        
        df = df.dropna()
        if df.empty:
            print("  FAILED: Data is empty after dropping NaN.")
            return

        print(f"  SUCCESS: {len(df)} rows found.")
        print(f"  Range: {df.index.min()} to {df.index.max()}")
        
    except Exception as e:
        print(f"  ERROR during download: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Check Yahoo Finance data availability with validation.")
    parser.add_argument("--symbol", default="QQQ", help="Ticker symbol (e.g., QQQ)")
    parser.add_argument("--interval", default="1m", help="Data interval (e.g., 1m, 5m, 1d)")
    parser.add_argument("--period", default="7d", help="Data period (e.g., 7d, 1mo, 3mo)")
    
    args = parser.parse_args()
    
    try:
        check_data_range(args.symbol, args.interval, args.period)
    except ValueError as ve:
        print(f"\n❌ Validation Error: {ve}")
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
