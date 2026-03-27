"""
Yahoo Finance 기반 시장 데이터 수집기.

KIS API 없이 yfinance만으로 수집 가능한 데이터:
  - US 지수 1분봉  (NASDAQ, S&P500 등)
  - KOSPI200 종목 1분봉  (심볼: {코드}.KS)
  - KOSPI200 종목 일봉   (장기 히스토리 백필용)

제약:
  - 1분봉: 최근 ~30일만 제공, 요청당 최대 7일
  - 투자자별 매매 동향은 yfinance 미지원 → investor_flow_collector.py 사용

저장 경로 (parquet_writer 키):
  nasdaq_1min        → market_data/us/nasdaq/1min/<YYYY-MM-DD>.parquet
  kospi200_1min      → market_data/kr/kospi200/1min/<YYYY-MM-DD>.parquet
  kospi200_daily_yf  → market_data/kr/kospi200/daily/<YYYY-MM-DD>.parquet  (일봉 전용)

사용법:
  # 오늘 전체 수집 (US 지수 + KOSPI200)
  uv run python src/yahoo_finance_collector.py

  # 날짜 지정
  uv run python src/yahoo_finance_collector.py --date 2025-03-07

  # 개별 스텝
  uv run python src/yahoo_finance_collector.py --step us_indices
  uv run python src/yahoo_finance_collector.py --step us_daily --start 2010-01-01
  uv run python src/yahoo_finance_collector.py --step kospi200_intraday
  uv run python src/yahoo_finance_collector.py --step kospi200_daily --start 2020-01-01
"""

from __future__ import annotations

import asyncio
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from loguru import logger

from data_validation import validate_intraday
from parquet_writer import MARKET_DATA_DIR, daily_path, write_parquet

# ──────────────────────────────────────────────
# 상수
# ──────────────────────────────────────────────

# 수집 대상 US 지수
US_INDICES: dict[str, str] = {
    "^IXIC": "nasdaq_1min",   # NASDAQ Composite
    "^NDX":  "nasdaq_1min",   # NASDAQ 100  (같은 폴더에 함께 저장)
    "^GSPC": "nasdaq_1min",   # S&P 500
}

# 수집 대상 US 일봉 (지수, ETF 등)
US_DAILY_SYMBOLS: list[str] = [
    "^IXIC",  # NASDAQ Composite
    "^NDX",   # NASDAQ 100
    "^GSPC",  # S&P 500
    "QQQ",    # Invesco QQQ Trust
]

# 글로벌 자산 (환율, 원자재, 변동성, 암호화폐)
GLOBAL_ASSETS: dict[str, str] = {
    "USDKRW=X": "fx_usdkrw",    # 원/달러 환율
    "JPYKRW=X": "fx_jpykrw",    # 원/엔 환율 (100엔 기준 환산 필요할 수 있음)
    "DX-Y.NYB": "dollar_index", # 달러 지수
    "CL=F":     "wti_crude",    # WTI 원유 선물
    "GC=F":     "gold_price",   # 금 선물
    "^VIX":     "vix_index",    # VIX (변동성 지수)
    "BTC-USD":  "crypto_btc",   # 비트코인
    "ETH-USD":  "crypto_eth",   # 이더리움
}

# yfinance 1분봉 제약
_MAX_DAYS_PER_CHUNK = 7

# 배치 다운로드 최대 종목 수 (너무 많으면 yfinance가 타임아웃)
_BATCH_SIZE = 50

_PARQUET_WRITER_PATHS: dict[str, Path] = {
    "nasdaq_1min":           MARKET_DATA_DIR / "us" / "nasdaq" / "1min",
    "us_daily":              MARKET_DATA_DIR / "us" / "daily",
    "global_daily":          MARKET_DATA_DIR / "global" / "daily",
    "kospi200_1min":         MARKET_DATA_DIR / "kr" / "kospi200" / "1min",
    "kospi200_daily_yf":     MARKET_DATA_DIR / "kr" / "kospi200" / "daily",
}


# ──────────────────────────────────────────────
# 헬퍼
# ──────────────────────────────────────────────

def _to_yf_kr(code: str) -> str:
    """6자리 종목코드 → yfinance 한국 심볼 (예: 005930 → 005930.KS)."""
    code = code.strip()
    if code.endswith(".KS") or code.endswith(".KQ"):
        return code
    return f"{code}.KS"


def _normalize_ts(df: pd.DataFrame) -> pd.DataFrame:
    """타임스탬프를 tz-naive UTC로 정규화."""
    if "timestamp" in df.columns and df["timestamp"].dt.tz is not None:
        df["timestamp"] = df["timestamp"].dt.tz_convert("UTC").dt.tz_localize(None)
    return df


def _download_1min(
    symbols: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    yfinance로 1분봉 다운로드 후 tidy DataFrame 반환.

    columns: timestamp, symbol, open, high, low, close, volume
    """
    end_exclusive = end + timedelta(days=1)

    raw = yf.download(
        tickers=symbols,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        interval="1m",
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    if raw.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    if len(symbols) == 1:
        # 단일 종목: MultiIndex 없음
        df = raw.reset_index()
        df = df.rename(columns={
            "Datetime": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        df["symbol"] = symbols[0]
        frames.append(df[["timestamp", "symbol", "open", "high", "low", "close", "volume"]])
    else:
        # 복수 종목: (Price, Ticker) MultiIndex columns
        for sym in symbols:
            try:
                sub = raw[sym].copy()
            except KeyError:
                logger.warning(f"  {sym}: 데이터 없음 (상장폐지 또는 심볼 오류)")
                continue
            if sub.empty:
                continue
            sub = sub.reset_index()
            sub = sub.rename(columns={
                "Datetime": "timestamp",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })
            sub["symbol"] = sym
            frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = _normalize_ts(combined)
    combined = combined.dropna(subset=["open", "close"])
    return combined


def _download_daily(
    symbols: list[str],
    start: date,
    end: date,
) -> pd.DataFrame:
    """
    yfinance로 일봉 다운로드 후 tidy DataFrame 반환.

    columns: timestamp(date), symbol, open, high, low, close, volume, trade_value
    """
    end_exclusive = end + timedelta(days=1)

    raw = yf.download(
        tickers=symbols,
        start=start.isoformat(),
        end=end_exclusive.isoformat(),
        interval="1d",
        auto_adjust=True,
        group_by="ticker",
        progress=False,
        threads=True,
    )

    if raw.empty:
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []

    if len(symbols) == 1:
        df = raw.reset_index()
        df = df.rename(columns={
            "Date": "timestamp",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        })
        df["symbol"] = symbols[0]
        df["trade_value"] = df["close"] * df["volume"]
        frames.append(df[["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_value"]])
    else:
        for sym in symbols:
            try:
                sub = raw[sym].copy()
            except KeyError:
                logger.warning(f"  {sym}: 데이터 없음")
                continue
            if sub.empty:
                continue
            sub = sub.reset_index()
            sub = sub.rename(columns={
                "Date": "timestamp",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            })
            sub["symbol"] = sym
            sub["trade_value"] = sub["close"] * sub["volume"]
            frames.append(sub[["timestamp", "symbol", "open", "high", "low", "close", "volume", "trade_value"]])

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.dropna(subset=["open", "close"])
    return combined


def _batches(items: list, size: int):
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ──────────────────────────────────────────────
# US 지수 수집
# ──────────────────────────────────────────────

async def collect_us_indices(
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> None:
    """
    US 지수 1분봉 수집.

    Args:
        symbols: yfinance 심볼 목록. 기본값: ['^IXIC', '^NDX', '^GSPC']
        start:   수집 시작일 (기본: 어제)
        end:     수집 종료일 (기본: 오늘)
    """
    if symbols is None:
        symbols = list(US_INDICES)
    if end is None:
        end = date.today()
    if start is None:
        start = end - timedelta(days=1)

    logger.info(f"US 지수 1분봉 수집: {symbols} / {start} → {end}")

    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=_MAX_DAYS_PER_CHUNK - 1), end)
        logger.debug(f"  청크: {current} → {chunk_end}")

        df = await asyncio.to_thread(_download_1min, symbols, current, chunk_end)

        if df.empty:
            logger.warning(f"  데이터 없음: {current} → {chunk_end}")
        else:
            for sym in df["symbol"].unique():
                sym_df = df[df["symbol"] == sym].copy()
                sym_df = validate_intraday(sym_df, symbol=sym)

                storage_key = US_INDICES.get(sym, "nasdaq_1min")
                for day, group in sym_df.groupby(sym_df["timestamp"].dt.date):
                    dest = daily_path(storage_key, str(day))
                    write_parquet(group.reset_index(drop=True), dest)

        current = chunk_end + timedelta(days=1)
        await asyncio.sleep(0.5)

    logger.success("US 지수 수집 완료")


# ──────────────────────────────────────────────
# US 일봉 (장기 히스토리 백필)
# ──────────────────────────────────────────────

async def collect_us_daily(
    start: date,
    end: date | None = None,
    symbols: list[str] | None = None,
) -> None:
    """
    US 지수 및 ETF 일봉 수집 (장기 히스토리 백필용).
    저장 경로: market_data/us/daily/<YYYY-MM-DD>.parquet

    Args:
        start:   수집 시작일
        end:     수집 종료일 (기본: 오늘)
        symbols: 쉼표 구분 심볼 목록. (기본: US_DAILY_SYMBOLS)
    """
    if end is None:
        end = date.today()
    if symbols is None:
        symbols = list(US_DAILY_SYMBOLS)

    logger.info(f"US 일봉 (Yahoo) {start} → {end}: {len(symbols)}개 종목")

    for i, batch in enumerate(_batches(symbols, _BATCH_SIZE), 1):
        logger.debug(f"  배치 {i}: {len(batch)}개 종목")

        df = await asyncio.to_thread(_download_daily, batch, start, end)
        if df.empty:
            logger.warning(f"  배치 {i}: 데이터 없음")
            continue

        df = validate_intraday(df)
        if df.empty:
            continue

        # 날짜별로 분할 저장
        df["_date"] = pd.to_datetime(df["timestamp"]).dt.date
        for day, group in df.groupby("_date"):
            dest = daily_path("us_daily", str(day))
            write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

        await asyncio.sleep(1.0)

    logger.success("US 일봉 저장 완료")


# ──────────────────────────────────────────────
# 글로벌 자산 일봉 (장기 히스토리 백필)
# ──────────────────────────────────────────────

async def collect_global_assets(
    start: date,
    end: date | None = None,
    symbols: list[str] | None = None,
) -> None:
    """
    글로벌 자산(환율, 원자재, 암호화폐 등) 일봉 수집.
    저장 경로: market_data/global/daily/<YYYY-MM-DD>.parquet

    Args:
        start:   수집 시작일
        end:     수집 종료일 (기본: 오늘)
        symbols: 심볼 목록 (기본: GLOBAL_ASSETS)
    """
    if end is None:
        end = date.today()
    if symbols is None:
        symbols = list(GLOBAL_ASSETS)

    logger.info(f"글로벌 자산 (Yahoo) {start} → {end}: {len(symbols)}개 종목")

    for i, batch in enumerate(_batches(symbols, _BATCH_SIZE), 1):
        df = await asyncio.to_thread(_download_daily, batch, start, end)
        if df.empty:
            logger.warning(f"  배치 {i}: 데이터 없음")
            continue

        df = validate_intraday(df)
        if df.empty:
            continue

        # 날짜별 분할 저장
        df["_date"] = pd.to_datetime(df["timestamp"]).dt.date
        for day, group in df.groupby("_date"):
            dest = daily_path("global_daily", str(day))
            write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

        await asyncio.sleep(1.0)

    logger.success("글로벌 자산 저장 완료")


# ──────────────────────────────────────────────
# KOSPI200 1분봉 수집
# ──────────────────────────────────────────────

def _load_component_codes(target_date: date) -> list[str]:
    """
    저장된 KOSPI200 구성종목 파일에서 코드 목록을 읽어 .KS 심볼로 변환.

    파일이 없으면 빈 리스트 반환 (경고 출력).
    """
    import pyarrow.parquet as pq

    path = MARKET_DATA_DIR / "metadata" / "kospi200_components" / f"{target_date}.parquet"
    if not path.exists():
        logger.warning(
            f"구성종목 파일 없음: {path}\n"
            f"먼저 실행: python src/kospi200_component_collector.py --date {target_date}"
        )
        return []

    table = pq.read_table(path, columns=["symbol"])
    codes = table.column("symbol").to_pylist()
    return [_to_yf_kr(c) for c in codes if c]


async def collect_kospi200_intraday(
    trade_date: date | None = None,
    codes: list[str] | None = None,
) -> None:
    """
    KOSPI200 종목 1분봉 수집 (yfinance 버전 — KIS API 불필요).

    Args:
        trade_date: 수집 날짜 (기본: 오늘). yfinance는 최근 ~30일만 지원.
        codes:      6자리 종목코드 목록. None이면 저장된 구성종목 파일 사용.
    """
    if trade_date is None:
        trade_date = date.today()

    if codes is not None:
        yf_symbols = [_to_yf_kr(c) for c in codes]
    else:
        yf_symbols = _load_component_codes(trade_date)

    if not yf_symbols:
        logger.error("수집할 종목이 없습니다.")
        return

    logger.info(f"KOSPI200 1분봉 (Yahoo) {trade_date}: {len(yf_symbols)}개 종목")

    dest = daily_path("kospi200_1min", str(trade_date))
    total_rows = 0

    for i, batch in enumerate(_batches(yf_symbols, _BATCH_SIZE), 1):
        logger.debug(f"  배치 {i}: {len(batch)}개 종목")

        df = await asyncio.to_thread(_download_1min, batch, trade_date, trade_date)
        if df.empty:
            logger.warning(f"  배치 {i}: 데이터 없음")
            continue

        df = validate_intraday(df)
        if not df.empty:
            write_parquet(df.reset_index(drop=True), dest)
            total_rows += len(df)

        await asyncio.sleep(1.0)  # yfinance 과부하 방지

    logger.success(f"KOSPI200 1분봉 저장 완료: {total_rows}행 → {dest}")


# ──────────────────────────────────────────────
# KOSPI200 일봉 (장기 히스토리 백필)
# ──────────────────────────────────────────────

async def collect_kospi200_daily(
    start: date,
    end: date | None = None,
    codes: list[str] | None = None,
    component_date: date | None = None,
) -> None:
    """
    KOSPI200 종목 일봉 수집 (장기 히스토리 백필용).

    yfinance 일봉은 수년치 데이터를 제공합니다.
    저장 경로: market_data/kr/kospi200/daily/<YYYY-MM-DD>.parquet

    Args:
        start:          수집 시작일
        end:            수집 종료일 (기본: 오늘)
        codes:          6자리 종목코드 목록. None이면 구성종목 파일 사용.
        component_date: 구성종목 파일 날짜 (기본: end 날짜)
    """
    if end is None:
        end = date.today()
    if component_date is None:
        component_date = end

    if codes is not None:
        yf_symbols = [_to_yf_kr(c) for c in codes]
    else:
        yf_symbols = _load_component_codes(component_date)

    if not yf_symbols:
        logger.error("수집할 종목이 없습니다.")
        return

    logger.info(f"KOSPI200 일봉 (Yahoo) {start} → {end}: {len(yf_symbols)}개 종목")
    _KOSPI200_DAILY_DIR.mkdir(parents=True, exist_ok=True)

    for i, batch in enumerate(_batches(yf_symbols, _BATCH_SIZE), 1):
        logger.debug(f"  배치 {i}: {len(batch)}개 종목")

        df = await asyncio.to_thread(_download_daily, batch, start, end)
        if df.empty:
            logger.warning(f"  배치 {i}: 데이터 없음")
            continue

        df = validate_intraday(df)   # 가격/볼륨 검증 재사용
        if df.empty:
            continue

        # 날짜별로 분할 저장
        df["_date"] = pd.to_datetime(df["timestamp"]).dt.date
        for day, group in df.groupby("_date"):
            dest = _KOSPI200_DAILY_DIR / f"{day}.parquet"
            write_parquet(group.drop(columns=["_date"]).reset_index(drop=True), dest)

        await asyncio.sleep(1.0)

    logger.success(f"KOSPI200 일봉 저장 완료 → {_KOSPI200_DAILY_DIR}")


# ──────────────────────────────────────────────
# 전체 파이프라인
# ──────────────────────────────────────────────

async def collect_all_yahoo(trade_date: date | None = None) -> None:
    """Yahoo Finance로 수집 가능한 전체 데이터를 수집."""
    if trade_date is None:
        trade_date = date.today()

    logger.info(f"=== Yahoo Finance 수집 파이프라인: {trade_date} ===")

    logger.info("1/2 — US 지수 1분봉")
    await collect_us_indices(start=trade_date, end=trade_date)

    logger.info("2/4 — US 일봉")
    await collect_us_daily(start=trade_date, end=trade_date)

    logger.info("3/4 — 글로벌 자산 (FX/원자재/코인)")
    await collect_global_assets(start=trade_date, end=trade_date)

    logger.info("4/4 — KOSPI200 1분봉")
    await collect_kospi200_intraday(trade_date=trade_date)

    logger.success("=== Yahoo Finance 수집 완료 ===")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Yahoo Finance 기반 시장 데이터 수집기",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
스텝 목록:
  us_indices        US 지수 1분봉 (^IXIC, ^NDX, ^GSPC)
  us_daily          US 지수 및 ETF 일봉 (장기 히스토리, --start 필수)
  global_daily      글로벌 자산(FX/원자재/코인) 일봉 (장기 히스토리, --start 필수)
  kospi200_intraday KOSPI200 종목 1분봉
  kospi200_daily    KOSPI200 종목 일봉 (장기 히스토리, --start 필수)

예시:
  python yahoo_finance_collector.py
  python yahoo_finance_collector.py --date 2025-03-07
  python yahoo_finance_collector.py --step kospi200_daily --start 2020-01-01
  python yahoo_finance_collector.py --step us_daily --start 2010-01-01
  python yahoo_finance_collector.py --step global_daily --start 2010-01-01
  python yahoo_finance_collector.py --step us_indices --symbols '^IXIC,^GSPC'
        """,
    )
    parser.add_argument("--date", default=None, help="기준 날짜 YYYY-MM-DD (기본: 오늘)")
    parser.add_argument("--start", default=None, help="시작 날짜 YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="종료 날짜 YYYY-MM-DD")
    parser.add_argument(
        "--step",
        choices=["us_indices", "us_daily", "global_daily", "kospi200_intraday", "kospi200_daily"],
        default=None,
        help="단일 스텝 실행",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="쉼표 구분 심볼 목록 (us_indices 전용, 예: '^IXIC,^GSPC')",
    )
    args = parser.parse_args()

    _trade_date = date.fromisoformat(args.date) if args.date else date.today()
    _start = date.fromisoformat(args.start) if args.start else None
    _end = date.fromisoformat(args.end) if args.end else date.today()
    _symbols = args.symbols.split(",") if args.symbols else None

    if args.step == "us_indices":
        asyncio.run(
            collect_us_indices(
                symbols=_symbols,
                start=_start or _trade_date,
                end=_end,
            )
        )
    elif args.step == "us_daily":
        if _start is None:
            parser.error("--step us_daily 는 --start 날짜가 필요합니다.")
        assert _start is not None
        asyncio.run(collect_us_daily(start=_start, end=_end, symbols=_symbols))
    elif args.step == "global_daily":
        if _start is None:
            parser.error("--step global_daily 는 --start 날짜가 필요합니다.")
        assert _start is not None
        asyncio.run(collect_global_assets(start=_start, end=_end, symbols=_symbols))
    elif args.step == "kospi200_intraday":
        asyncio.run(collect_kospi200_intraday(trade_date=_trade_date))
    elif args.step == "kospi200_daily":
        if _start is None:
            parser.error("--step kospi200_daily 는 --start 날짜가 필요합니다.")
        asyncio.run(collect_kospi200_daily(start=_start, end=_end, codes=_symbols))
    else:
        asyncio.run(collect_all_yahoo(trade_date=_trade_date))
