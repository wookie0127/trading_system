"""
Market context data collector.

수집 대상:
  US Market  : S&P500 close, NASDAQ close, SOX close, VIX close
  KR Market  : KOSPI open, KOSPI close
  Investor   : 외국인 순매수, 기관 순매수 (종목 코드 지정)
  FX         : USD/KRW close

사용법:
    python market_context_collector.py
"""
import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[2]))  # src/ 패키지 루트
del _sys, _Path


from datetime import datetime, timedelta

import pandas as pd
from loguru import logger

from core.kis_market_handler import MarketHandler

# ---------------------------------------------------------------------------
# 수집 대상 심볼 정의
# ---------------------------------------------------------------------------

OVERSEAS_INDICES = {
    "SP500":   ".SPX",
    "NASDAQ":  ".IXIC",
    "SOX":     ".SOX",
    "VIX":     ".VIX",
}

DOMESTIC_INDICES = {
    "KOSPI":   "0001",
}

FX_SYMBOLS = {
    "USDKRW": "FX@KRW",
}

# 외국인/기관 순매수를 수집할 종목 코드 (필요에 따라 확장)
INVESTOR_FLOW_CODES: list[str] = [
    "005930",  # 삼성전자
]


# ---------------------------------------------------------------------------
# 수집 함수
# ---------------------------------------------------------------------------

def collect_overseas_indices(
    handler: MarketHandler,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, pd.DataFrame]:
    """해외지수 일봉 수집."""
    results: dict[str, pd.DataFrame] = {}
    for name, symbol in OVERSEAS_INDICES.items():
        logger.info("Fetching overseas index: %s (%s)", name, symbol)
        df = handler.fetch_overseas_index_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if df.empty:
            logger.warning("  -> No data for %s", name)
        else:
            logger.success("  -> %d rows", len(df))
        results[name] = df
    return results


def collect_domestic_indices(
    handler: MarketHandler,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, pd.DataFrame]:
    """국내지수 일봉 수집."""
    results: dict[str, pd.DataFrame] = {}
    for name, symbol in DOMESTIC_INDICES.items():
        logger.info("Fetching domestic index: %s (%s)", name, symbol)
        df = handler.fetch_domestic_index_daily(
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
        )
        if df.empty:
            logger.warning("  -> No data for %s", name)
        else:
            logger.success("  -> %d rows", len(df))
        results[name] = df
    return results


def collect_investor_flow(
    handler: MarketHandler,
    codes: list[str],
) -> dict[str, dict]:
    """외국인/기관 당일 매매동향 수집."""
    results: dict[str, dict] = {}
    for code in codes:
        logger.info("Fetching investor flow: %s", code)
        data = handler.fetch_investor_flow(code=code)
        results[code] = data
    return results


def collect_fx_rates(
    handler: MarketHandler,
    start_date: datetime,
    end_date: datetime,
) -> dict[str, pd.DataFrame]:
    """환율 일봉 수집."""
    results: dict[str, pd.DataFrame] = {}
    for name, fx_code in FX_SYMBOLS.items():
        logger.info("Fetching FX rate: %s (%s)", name, fx_code)
        df = handler.fetch_fx_rate_daily(
            fx_code=fx_code,
            start_date=start_date,
            end_date=end_date,
        )
        if df.empty:
            logger.warning("  -> No data for %s", name)
        else:
            logger.success("  -> %d rows", len(df))
        results[name] = df
    return results


# ---------------------------------------------------------------------------
# 결과 출력 헬퍼
# ---------------------------------------------------------------------------

def _print_latest(name: str, df: pd.DataFrame, close_col: str, open_col: str | None = None) -> None:
    if df.empty:
        print(f"  {name}: N/A")
        return
    latest = df.iloc[-1]
    close = latest.get(close_col, "?")
    if open_col:
        open_ = latest.get(open_col, "?")
        print(f"  {name}: open={open_}, close={close}  ({latest['bsop_date'].date()})")
    else:
        print(f"  {name}: close={close}  ({latest['bsop_date'].date()})")


# ---------------------------------------------------------------------------
# 메인
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    end_date = datetime.today()
    start_date = end_date - timedelta(days=30)

    handler = MarketHandler()

    print("\n=== US Market Indices ===")
    overseas = collect_overseas_indices(handler, start_date, end_date)
    _print_latest("S&P500",  overseas.get("SP500",  pd.DataFrame()), "clpr")
    _print_latest("NASDAQ",  overseas.get("NASDAQ", pd.DataFrame()), "clpr")
    _print_latest("SOX",     overseas.get("SOX",    pd.DataFrame()), "clpr")
    _print_latest("VIX",     overseas.get("VIX",    pd.DataFrame()), "clpr")

    print("\n=== KR Market Indices ===")
    domestic = collect_domestic_indices(handler, start_date, end_date)
    _print_latest("KOSPI", domestic.get("KOSPI", pd.DataFrame()), "bstp_nmix_clpr", "bstp_nmix_oprc")

    print("\n=== Investor Flow (today) ===")
    flows = collect_investor_flow(handler, INVESTOR_FLOW_CODES)
    for code, data in flows.items():
        output = data.get("output") or {}
        frgn_ntby = output.get("frgn_ntby_qty", "?")
        orgn_ntby = output.get("orgn_ntby_qty", "?")
        print(f"  {code}: 외국인 순매수={frgn_ntby}, 기관 순매수={orgn_ntby}")

    print("\n=== FX Rates ===")
    fx = collect_fx_rates(handler, start_date, end_date)
    usdkrw = fx.get("USDKRW", pd.DataFrame())
    if not usdkrw.empty:
        latest = usdkrw.iloc[-1]
        print(f"  USD/KRW: close={latest.get('clos', '?')}  ({latest['bsop_date'].date()})")
    else:
        print("  USD/KRW: N/A")
