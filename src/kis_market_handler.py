"""
Handle market using KIS
"""

from datetime import datetime, timedelta
import time

import httpx
import json
import pandas as pd
from kis_config import API_ROOT, CODE_PATH_BOOK
from kis_auth_handler import KISAuthHandler
from loguru import logger


def read_json(json_path: str) -> list[dict]:
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_codes() -> pd.DataFrame:
    dfs = []
    for market in CODE_PATH_BOOK:
        json_path = CODE_PATH_BOOK[market]
        df = pd.DataFrame.from_records(read_json(json_path))
        df["marekt"] = market
        dfs.append(df)
    return pd.concat(dfs)


class MarketHandler(KISAuthHandler):
    def __init__(self):
        super().__init__()
        self._code_df = load_codes()

    @staticmethod
    def _format_date(value: datetime | str) -> str:
        """Return YYYYMMDD strings from datetime or raw string inputs."""

        if isinstance(value, str):
            return value
        return value.strftime("%Y%m%d")

    def get_code(self, company_name: str):
        code = self._code_df[
            (self._code_df["ko_name"] == company_name)
            | (self._code_df["en_name"] == company_name)
        ]
        if code.empty:
            logger.warning(f"No found code about {company_name}")
            return
        return code.iloc[0]["code"]

    def inquire_domestic_stock(self, code: str):
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-price"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST01010100",
        }
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": code}
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        return res.json()

    def _fetch_domestic_chartprice_chunk(
        self,
        code: str,
        start_date: datetime | str,
        end_date: datetime | str,
        period_code: str = "D",
        adjusted: bool = True,
    ) -> list[dict]:
        """Fetch up to 100 OHLC rows for the requested window."""

        if code is None:
            raise ValueError("`code` is required to fetch chart data.")

        start_str = self._format_date(start_date)
        end_str = self._format_date(end_date)

        if start_str > end_str:
            raise ValueError("`start_date` must be earlier than `end_date`.")

        endpoint = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03010100",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start_str,
            "FID_INPUT_DATE_2": end_str,
            "FID_PERIOD_DIV_CODE": period_code,
            "FID_ORG_ADJ_PRC": "0" if adjusted else "1",
        }
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        payload = res.json()

        output = payload.get("output2") or []
        if not isinstance(output, list):
            logger.warning(
                "Unexpected response format while fetching chart price for %s", code
            )
            return []
        return output

    def fetch_domestic_daily_prices(
        self,
        code: str,
        min_rows: int = 250,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        adjusted: bool = True,
        chunk_span_days: int = 160,
    ) -> pd.DataFrame:
        """Fetch and stitch together historical daily bars for a code.

        The API returns at most 100 rows per call, so we iteratively walk
        backwards in time until we satisfy `min_rows` or reach `start_date`.
        """

        if end_date is None:
            end_date = datetime.today()
        if start_date is None:
            start_date = end_date - timedelta(days=chunk_span_days * 3)

        rows: list[dict] = []
        current_end = end_date
        start_bound = start_date

        while current_end >= start_bound and len(rows) < min_rows:
            current_start = max(
                start_bound, current_end - timedelta(days=chunk_span_days)
            )
            chunk = self._fetch_domestic_chartprice_chunk(
                code=code,
                start_date=current_start,
                end_date=current_end,
                adjusted=adjusted,
            )
            if not chunk:
                break

            rows.extend(chunk)
            # Prepare for the next request using the oldest date from this chunk.
            oldest = min(
                item["stck_bsop_date"] for item in chunk if item.get("stck_bsop_date")
            )
            current_end = datetime.strptime(oldest, "%Y%m%d") - timedelta(days=1)
            if current_end < start_bound:
                break

            time.sleep(0.2)

        if not rows:
            return pd.DataFrame()

        frame = pd.DataFrame(rows).drop_duplicates(subset="stck_bsop_date")
        if "stck_bsop_date" in frame.columns:
            frame["stck_bsop_date"] = pd.to_datetime(
                frame["stck_bsop_date"], format="%Y%m%d"
            )
            frame = frame.sort_values("stck_bsop_date").reset_index(drop=True)

        numeric_cols = [
            col
            for col in ["stck_clpr", "stck_oprc", "stck_hgpr", "stck_lwpr", "acml_vol"]
            if col in frame.columns
        ]
        for col in numeric_cols:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")

        return frame

    def _compute_moving_averages(
        self, df: pd.DataFrame, windows: tuple[int, ...]
    ) -> pd.DataFrame:
        if df.empty:
            return df
        close_col = "stck_clpr"
        if close_col not in df.columns:
            raise ValueError("Missing 'stck_clpr' column in chart data.")
        for window in windows:
            df[f"ma_{window}"] = df[close_col].rolling(window=window).mean()
        return df

    def scan_alignment_candidates(
        self,
        markets: list[str] | None = None,
        limit: int | None = None,
        min_rows: int = 250,
    ) -> pd.DataFrame:
        """Return stocks where MA224 < MA112 at the latest available date."""

        if markets:
            candidates = self._code_df[self._code_df["marekt"].isin(markets)]
        else:
            candidates = self._code_df

        matches: list[dict] = []
        for _, row in candidates.iterrows():
            code = row["code"]
            try:
                prices = self.fetch_domestic_daily_prices(code=code, min_rows=min_rows)
                prices = self._compute_moving_averages(prices, windows=(112, 224))
                valid = prices.dropna(subset=["ma_112", "ma_224"])
                if valid.empty:
                    continue
                latest = valid.iloc[-1]
            except Exception as exc:  # noqa: BLE001
                logger.warning("Skipping %s (%s): %s", code, row["ko_name"], exc)
                continue

            if latest["ma_224"] < latest["ma_112"]:
                matches.append(
                    {
                        "code": code,
                        "ko_name": row["ko_name"],
                        "en_name": row["en_name"],
                        "last_date": latest["stck_bsop_date"],
                        "close": latest["stck_clpr"],
                        "ma_112": latest["ma_112"],
                        "ma_224": latest["ma_224"],
                    }
                )

            if limit and len(matches) >= limit:
                break

        return pd.DataFrame(matches)

    def fetch_overseas_index_daily(
        self,
        symbol: str,
        start_date: datetime | str | None = None,
        end_date: datetime | str | None = None,
    ) -> pd.DataFrame:
        """해외지수 일봉 조회 (S&P500, NASDAQ, SOX, VIX 등).

        symbol 예시: '.SPX', '.IXIC', '.SOX', '.VIX'
        응답 컬럼: bsop_date, clpr, oprc, hgpr, lwpr
        """
        if end_date is None:
            end_date = datetime.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365)

        endpoint = "uapi/overseas-price/v1/quotations/inquire-daily-indexchartprice"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKUP03500100",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "N",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": self._format_date(start_date),
            "FID_INPUT_DATE_2": self._format_date(end_date),
            "FID_PERIOD_DIV_CODE": "D",
        }
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        payload = res.json()

        output = payload.get("output2") or []
        if not isinstance(output, list) or not output:
            logger.warning("No data returned for overseas index %s", symbol)
            return pd.DataFrame()

        frame = pd.DataFrame(output).drop_duplicates(subset="bsop_date")
        frame["bsop_date"] = pd.to_datetime(frame["bsop_date"], format="%Y%m%d")
        frame = frame.sort_values("bsop_date").reset_index(drop=True)
        for col in ["clpr", "oprc", "hgpr", "lwpr"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def fetch_domestic_index_daily(
        self,
        symbol: str,
        start_date: datetime | str | None = None,
        end_date: datetime | str | None = None,
    ) -> pd.DataFrame:
        """국내지수 일봉 조회 (KOSPI, KOSDAQ 등).

        symbol 예시: '0001' = KOSPI, '1001' = KOSDAQ
        응답 컬럼: bsop_date, bstp_nmix_oprc, bstp_nmix_clpr, bstp_nmix_hgpr, bstp_nmix_lwpr
        """
        if end_date is None:
            end_date = datetime.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365)

        endpoint = "uapi/domestic-stock/v1/quotations/inquire-daily-indexchartprice"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKUP03500100",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "U",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": self._format_date(start_date),
            "FID_INPUT_DATE_2": self._format_date(end_date),
            "FID_PERIOD_DIV_CODE": "D",
        }
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        payload = res.json()

        output = payload.get("output2") or []
        if not isinstance(output, list) or not output:
            logger.warning("No data returned for domestic index %s", symbol)
            return pd.DataFrame()

        frame = pd.DataFrame(output).drop_duplicates(subset="bsop_date")
        frame["bsop_date"] = pd.to_datetime(frame["bsop_date"], format="%Y%m%d")
        frame = frame.sort_values("bsop_date").reset_index(drop=True)
        for col in ["bstp_nmix_oprc", "bstp_nmix_clpr", "bstp_nmix_hgpr", "bstp_nmix_lwpr", "acml_vol"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def fetch_investor_flow(self, code: str) -> dict:
        """종목별 당일 투자자(외국인/기관/개인) 매매동향 조회.

        TR: FHKST01010900
        당일 데이터만 반환. 기간 조회는 수집 스크립트에서 날짜를 순회하여 호출.
        """
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-investor"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST01010900",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
        }
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        return res.json()

    def fetch_fx_rate_daily(
        self,
        fx_code: str,
        start_date: datetime | str | None = None,
        end_date: datetime | str | None = None,
    ) -> pd.DataFrame:
        """환율 일봉 조회.

        fx_code 예시: 'FX@KRW' (USD/KRW)
        응답 컬럼: bsop_date, clpr, oprc, hgpr, lwpr
        ※ KIS API 환율 심볼/마켓코드는 실계좌 환경에서 테스트 필요.
        """
        if end_date is None:
            end_date = datetime.today()
        if start_date is None:
            start_date = end_date - timedelta(days=365)

        endpoint = "uapi/overseas-price/v1/quotations/inquire-daily-itemchartprice"
        token = self.get_valid_token()
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS76240000",
        }
        params = {
            "AUTH": "",
            "EXCD": "FX",
            "SYMB": fx_code,
            "GUBN": "0",
            "BYMD": self._format_date(end_date),
            "MODP": "0",
        }
        res = httpx.get(
            f"{API_ROOT}/{endpoint}", headers=headers, params=params, timeout=300
        )
        res.raise_for_status()
        payload = res.json()

        output = payload.get("output2") or []
        if not isinstance(output, list) or not output:
            logger.warning("No FX data returned for %s", fx_code)
            return pd.DataFrame()

        frame = pd.DataFrame(output).drop_duplicates(subset="xymd")
        frame["xymd"] = pd.to_datetime(frame["xymd"], format="%Y%m%d")
        frame = frame.rename(columns={"xymd": "bsop_date"})
        frame = frame.sort_values("bsop_date").reset_index(drop=True)
        for col in ["clos", "open", "high", "low"]:
            if col in frame.columns:
                frame[col] = pd.to_numeric(frame[col], errors="coerce")
        return frame

    def inquire_oversee_stock(self, code: str):
        endpoint = "uapi/overseas-price/v1/quotations/price"
        token = self.get_valid_token()
        headers_overseas = {
            "Content-Type": "application/json; charset=utf-8",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS00000300",  # 해외주식 현재가조회 TR
        }
        params_overseas = {"AUTH": "", "EXCD": "NAS", "SYMB": code}
        res = httpx.get(
            f"{API_ROOT}/{endpoint}",
            headers=headers_overseas,
            params=params_overseas,
            timeout=300,
        )
        res.raise_for_status()
        return res.json()


if __name__ == "__main__":
    print("Start")
    market_handler = MarketHandler()
    print("ready marekt handler")
    # 국내주식 테스트
    company_name = "삼성전자"
    code = market_handler.get_code(company_name=company_name)
    price = market_handler.inquire_domestic_stock(code=code)
    print(f"=== {company_name} ===")
    print(price)
    # 해외주식 테스트
    code = "AAPL"
    price = market_handler.inquire_oversee_stock(code=code)
    print(f"=== {code} ===")
    print(price)

    # 간단한 112/224선 정렬 조건 스캐너 예시
    candidates = market_handler.scan_alignment_candidates(markets=["kospi"], limit=5)
    print("=== MA Alignment Candidates ===")
    if not candidates.empty:
        print(candidates[["code", "ko_name", "last_date", "close", "ma_112", "ma_224"]])
    else:
        print("No matches found with the current constraints.")
