import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

import json
import os
import re
import time
from pathlib import Path

import httpx
import yaml
from dotenv import load_dotenv
import FinanceDataReader as fdr
from pykrx import stock as pykrx_stock

from core.kis_auth_handler import KISAuthHandler
from core.kis_config import CODE_PATH_BOOK
from loguru import logger

CURRENT_DIR = Path(__file__).parent

# Load environment variables from .ssh/kis
KEY_PATH = Path.home() / ".ssh" / "kis"
load_dotenv(KEY_PATH)

# Load config from config.yaml if it exists
CONFIG_PATH = CURRENT_DIR.parent / "config.yaml"
if CONFIG_PATH.exists():
    with open(CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)
        if config:
            for k, v in config.items():
                os.environ[k] = str(v)


def read_json(json_path: str) -> list[dict]:
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        components = payload.get("components")
        if isinstance(components, list):
            normalized: list[dict] = []
            for item in components:
                if not isinstance(item, dict):
                    continue
                normalized.append(
                    {
                        "code": item.get("symbol") or item.get("code"),
                        "ko_name": item.get("name") or item.get("ko_name"),
                        "en_name": item.get("name") or item.get("en_name"),
                    }
                )
            return normalized

    return []


def load_codes() -> list[dict]:
    """모든 시장의 종목 코드를 취합하여 반환"""
    all_codes = []
    for market in CODE_PATH_BOOK:
        json_path = CODE_PATH_BOOK[market]
        if Path(json_path).exists():
            all_codes.extend(read_json(json_path))
    return all_codes


class MarketHandler(KISAuthHandler):
    def __init__(self, exchange: str = "서울"):
        super().__init__()
        self.reference_dir = CURRENT_DIR.parent.parent / "data" / "reference"
        self.reference_dir.mkdir(parents=True, exist_ok=True)
        self.krx_code_cache_path = self.reference_dir / "krx_all_symbols.json"
        self._code_df = self._load_all_codes()
        self.exchange = exchange

        # 계좌 설정 (8자리-2자리 분리)
        cano = self.account_number
        prdt_cd = self.account_product_code

        if "-" in cano:
            self.acc_no_prefix = cano.split("-")[0]
            self.acc_no_postfix = cano.split("-")[1]
        else:
            self.acc_no_prefix = cano
            self.acc_no_postfix = prdt_cd.zfill(2)

        if not self.acc_no_prefix:
            logger.warning("No KIS account number configured for profile={}", self.credential_profile)

    def _load_all_codes(self) -> list[dict]:
        records = load_codes()
        records.extend(self._load_cached_krx_codes())
        return self._dedupe_code_records(records)

    def get_code(self, company_name: str) -> str | None:
        """종목명으로 종목 코드 찾기"""
        candidates = self._build_code_lookup_candidates(company_name)
        records = self.search_symbols(company_name, limit=1)
        if records:
            return records[0].get("code")

        refreshed_records = self.search_symbols(company_name, limit=1, refresh=True)
        if refreshed_records:
            return refreshed_records[0].get("code")

        logger.warning(f"No code found for company: {company_name.strip()} candidates={candidates}")
        return None

    def search_symbols(self, query: str, limit: int = 20, refresh: bool = False) -> list[dict]:
        """로컬 캐시와 KRX 전체 종목 캐시를 이용해 종목 검색"""
        if refresh:
            try:
                self.refresh_krx_codes()
            except Exception as exc:
                logger.warning("Failed to refresh KRX symbols during search: {}", exc)

        candidates = self._build_code_lookup_candidates(query)
        records = self._search_code_records(candidates, limit=limit)
        if records or refresh:
            return records

        if not self._load_cached_krx_codes():
            try:
                self.refresh_krx_codes()
            except Exception as exc:
                logger.warning("Failed to refresh initial KRX symbol cache: {}", exc)
                return records
            return self._search_code_records(candidates, limit=limit)

        return records

    def refresh_krx_codes(self) -> list[dict]:
        """KRX 전체 상장 종목 캐시 갱신"""
        records = self._fetch_krx_codes_from_fdr()
        if not records:
            records = self._fetch_krx_codes_from_pykrx()

        if not records:
            raise RuntimeError("Failed to refresh KRX symbol cache from both FDR and pykrx")

        normalized = self._dedupe_code_records(records)
        with open(self.krx_code_cache_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "count": len(normalized),
                    "symbols": normalized,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )

        self._code_df = self._dedupe_code_records(load_codes() + normalized)
        logger.info("Refreshed KRX symbol cache: {} symbols", len(normalized))
        return normalized

    def _fetch_krx_codes_from_fdr(self) -> list[dict]:
        records: list[dict] = []
        try:
            df = fdr.StockListing("KRX")
        except Exception as exc:
            logger.warning("FinanceDataReader KRX listing failed: {}", exc)
            return records

        for item in df.to_dict("records"):
            symbol = str(item.get("Code") or item.get("Symbol") or "").zfill(6)
            name = str(item.get("Name") or "").strip()
            market = str(item.get("Market") or "KRX").strip()
            if not symbol or not name:
                continue
            records.append(
                {
                    "code": symbol,
                    "ko_name": name,
                    "en_name": name,
                    "market": market,
                    "source": "fdr",
                }
            )

        return records

    def _fetch_krx_codes_from_pykrx(self) -> list[dict]:
        markets = ("KOSPI", "KOSDAQ", "KONEX")
        records: list[dict] = []

        try:
            for market in markets:
                tickers = pykrx_stock.get_market_ticker_list(market=market)
                for ticker in tickers:
                    name = pykrx_stock.get_market_ticker_name(ticker)
                    if not name:
                        continue
                    records.append(
                        {
                            "code": ticker,
                            "ko_name": name,
                            "en_name": name,
                            "market": market,
                            "source": "pykrx",
                        }
                    )
        except Exception as exc:
            logger.warning("pykrx KRX listing failed: {}", exc)
            return []

        return records

    @staticmethod
    def _build_code_lookup_candidates(company_name: str) -> list[str]:
        raw = company_name.strip()
        if not raw:
            return []

        candidates: list[str] = []

        def add(value: str):
            normalized = re.sub(r"\s+", " ", value).strip(" '\"[]{}")
            if normalized and normalized not in candidates:
                candidates.append(normalized)

        add(raw)
        add(raw.replace(" ", ""))

        for inner in re.findall(r"\(([^)]+)\)", raw):
            add(inner)
            add(inner.replace(" ", ""))

        for part in re.split(r"[,/·→\-]|(?:\(|\))", raw):
            add(part)

        for suffix in ("그룹", "그룹주", "계열", "계열주", "테마", "관련주", "섹터"):
            if raw.endswith(suffix):
                add(raw[: -len(suffix)])

        return candidates

    def _search_code_records(self, candidates: list[str], limit: int = 20) -> list[dict]:
        exact_matches: list[dict] = []
        partial_matches: list[dict] = []

        for stock in self._code_df:
            code = str(stock.get("code") or "")
            ko_name = str(stock.get("ko_name") or "")
            en_name = str(stock.get("en_name") or "")
            searchable = [code, ko_name, en_name, ko_name.replace(" ", ""), en_name.replace(" ", "")]

            for candidate in candidates:
                if any(value == candidate for value in searchable if value):
                    exact_matches.append(stock)
                    break
            else:
                merged = " ".join(searchable)
                if any(candidate and candidate in merged for candidate in candidates):
                    partial_matches.append(stock)

        merged = self._dedupe_code_records(exact_matches + partial_matches)
        return merged[:limit]

    def _load_cached_krx_codes(self) -> list[dict]:
        if not self.krx_code_cache_path.exists():
            return []

        try:
            with open(self.krx_code_cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except Exception:
            return []

        if isinstance(payload, dict) and isinstance(payload.get("symbols"), list):
            return payload["symbols"]
        if isinstance(payload, list):
            return payload
        return []

    @staticmethod
    def _dedupe_code_records(records: list[dict]) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for record in records:
            code = str(record.get("code") or "").strip()
            ko_name = str(record.get("ko_name") or "").strip()
            key = (code, ko_name)
            if not code or key in seen:
                continue
            seen.add(key)
            deduped.append(record)

        return deduped

    @staticmethod
    def _to_int(value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(float(str(value).replace(",", "").strip()))
        except (TypeError, ValueError):
            return 0

    def fetch_price(self, symbol: str) -> dict:
        """현재가 조회 (국내/해외 통합)"""
        if self.exchange == "서울":
            return self.fetch_domestic_price(symbol)
        else:
            return self.fetch_oversea_price(symbol)

    def fetch_domestic_price(self, symbol: str) -> dict:
        """국내 주식 현재가 조회"""
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-price"
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": symbol}
        return self._request_with_auth(
            "GET",
            endpoint,
            tr_id="FHKST01010100",
            params=params,
        )

    def fetch_domestic_future_board(self, market_cls_code: str = "MKI") -> dict:
        """국내 지수선물 전광판 조회"""
        endpoint = "uapi/domestic-futureoption/v1/quotations/display-board-futures"
        params = {
            "FID_COND_MRKT_DIV_CODE": "F",
            "FID_COND_SCR_DIV_CODE": "20503",
            "FID_COND_MRKT_CLS_CODE": market_cls_code,
        }
        return self._request_with_auth(
            "GET",
            endpoint,
            tr_id="FHPIF05030200",
            params=params,
        )

    def fetch_domestic_future_price(self, shtn_pdno: str, market_cls_code: str = "MKI") -> int:
        """국내 지수선물 현재가 조회"""
        data = self.fetch_domestic_future_board(market_cls_code=market_cls_code)
        rows = data.get("output") or data.get("output1") or []
        if isinstance(rows, dict):
            rows = [rows]

        for row in rows:
            if not isinstance(row, dict):
                continue
            row_code = str(row.get("futs_shrn_iscd") or row.get("shrn_iscd") or "").strip()
            if row_code and row_code != shtn_pdno:
                continue
            price = self._to_int(row.get("futs_prpr") or row.get("futs_antc_cnpr") or row.get("stck_prpr"))
            if price > 0:
                return price

        if rows:
            row = rows[0]
            if isinstance(row, dict):
                return self._to_int(row.get("futs_prpr") or row.get("futs_antc_cnpr") or row.get("stck_prpr"))
        return 0

    def fetch_oversea_price(self, symbol: str) -> dict:
        """해외 주식 현재가 조회"""
        endpoint = "uapi/overseas-price/v1/quotations/price"
        # KIS 해외 거래소 코드 반영 (나스닥: NAS, 뉴욕: NYS)
        excd = "NAS" if self.exchange == "나스닥" else "NYS"
        params = {"AUTH": "", "EXCD": excd, "SYMB": symbol}
        return self._request_with_auth(
            "GET",
            endpoint,
            tr_id="HHDFS00000300",
            params=params,
        )

    def fetch_ohlcv(self, symbol: str, timeframe: str = "D", start_day: str = "", end_day: str = "", adj_price: bool = True, limit: int = 100) -> list[dict]:
        """과거 주가 데이터 조회 (OHLCV) - 페이지네이션 지원"""
        all_data = []
        current_end_day = end_day if end_day else time.strftime("%Y%m%d")
        
        # 반복 호출로 데이터 수집 (한 번에 100개씩)
        while len(all_data) < limit:
            if self.exchange == "서울":
                data = self.fetch_domestic_ohlcv(symbol, timeframe, start_day, current_end_day, adj_price)
            else:
                data = self.fetch_oversea_ohlcv(symbol, timeframe, start_day, current_end_day, adj_price)
            
            if not data:
                break
            
            all_data.extend(data)
            
            # 더 이상 가져올 데이터가 없거나 100개 미만이면 종료
            if len(data) < 100:
                break
            
            # 다음 조회를 위해 마지막 날짜 업데이트 (가장 예전 날짜의 하루 전)
            last_date_str = data[-1].get("stck_bsop_date") or data[-1].get("xymd")
            if not last_date_str:
                break
                
            from datetime import datetime, timedelta
            last_dt = datetime.strptime(last_date_str, "%Y%m%d")
            current_end_day = (last_dt - timedelta(days=1)).strftime("%Y%m%d")
            
            # 시작일보다 이전으로 넘어가면 종료
            if start_day and current_end_day < start_day:
                break
                
            time.sleep(0.1) # 안정적인 수집을 위한 짧은 대기

        return all_data[:limit]

    def fetch_domestic_ohlcv(self, symbol: str, timeframe: str = "D", start_day: str = "", end_day: str = "", adj_price: bool = True) -> list[dict]:
        """국내 주식 일/주/월봉 조회 (단일 페이지 100건)"""
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03010100",
        }
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_DATE_1": start_day if start_day else "19900101",
            "FID_INPUT_DATE_2": end_day if end_day else time.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": timeframe,
            "FID_ORG_ADJ_PRC": "0" if adj_price else "1",
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json().get("output2", [])

    def fetch_oversea_ohlcv(self, symbol: str, timeframe: str = "D", start_day: str = "", end_day: str = "", adj_price: bool = True) -> list[dict]:
        """해외 주식 일/주/월봉 조회 (단일 페이지 100건)"""
        endpoint = "uapi/overseas-price/v1/quotations/dailyprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS76240000",
        }
        excd = "NAS" if self.exchange == "나스닥" else "NYS"
        
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symbol,
            "GUBN": "0" if timeframe == "D" else ("1" if timeframe == "W" else "2"),
            "BYMD": end_day if end_day else time.strftime("%Y%m%d"),
            "MODP": "1" if adj_price else "0",
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json().get("output2", [])

    def fetch_oversea_index_ohlcv(self, symbol: str, timeframe: str = "D", start_day: str = "", end_day: str = "") -> list[dict]:
        """해외 지수(나스닥, S&P500 등) 일/주/월봉 조회"""
        endpoint = "uapi/overseas-price/v1/quotations/inquire-daily-chartprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03030100",
        }
        
        params = {
            "FID_COND_MRKT_DIV_CODE": "N",
            "FID_INPUT_ISCD": symbol, # 예: .COMP, .SPX 등
            "FID_INPUT_DATE_1": start_day if start_day else "20100101",
            "FID_INPUT_DATE_2": end_day if end_day else time.strftime("%Y%m%d"),
            "FID_PERIOD_DIV_CODE": timeframe,
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        data = res.json()
        
        if data.get("rt_cd") != "0":
            logger.error(f"KIS Index API Error: {data.get('msg1')} (Symbol: {symbol})")
            
        return data.get("output2", [])

    def fetch_balance(self) -> dict:
        """전체 잔고 조회 (연속 조회 처리)"""
        if self.exchange != "서울":
            return {"msg1": "해외 잔고 조회는 아직 지원되지 않습니다."}

        output = {"output1": [], "output2": []}
        fk100, nk100 = "", ""

        for _ in range(5):  # 최대 5페이지 혹은 재시도
            data = self._fetch_balance_step(fk100, nk100)

            # KIS 특유의 '조회이후 자료변경' 에러(rt_cd='7') 대응: 잠시 후 재시도
            if data.get("rt_cd") == "7":
                logger.warning("KIS API: 자료 변경됨. 0.5초 후 재시도...")
                time.sleep(0.5)
                continue

            if data.get("rt_cd") != "0":
                return data

            output["output1"].extend(data.get("output1", []))
            # output2는 리스트 타입으로 유지 (보통 [ {요약정보} ] 형태)
            if isinstance(data.get("output2"), list):
                output["output2"] = data.get("output2", [])
            elif isinstance(data.get("output2"), dict):
                output["output2"] = [data.get("output2", {})]

            # 연속 조회 키 업데이트
            if data.get("tr_cont") in ["M", "D"]:
                fk100 = data.get("ctx_area_fk100", "")
                nk100 = data.get("ctx_area_nk100", "")
                time.sleep(0.2) # API 과부하 방지
            else:
                break
        return output

    def fetch_intraday_candles(self, symbol: str, timeframe: str = "1", end_time: str = "") -> list[dict]:
        """분봉 데이터 조회 (국내/해외 통합)"""
        if self.exchange == "서울":
            return self.fetch_domestic_intraday(symbol, timeframe, end_time)
        else:
            return self.fetch_oversea_intraday(symbol, timeframe, end_time)

    def fetch_domestic_intraday(self, symbol: str, timeframe: str = "1", end_time: str = "") -> list[dict]:
        """국내 주식 당일 분봉 조회 (1분, 3분, 5분 등)"""
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-time-itemchartprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST03010200",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
            "FID_INPUT_HOUR_1": end_time,
            "FID_ETC_CLS_CODE": "",
            "FID_PW_DATA_INCU_YN": "Y",
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json().get("output2", [])

    def fetch_oversea_intraday(self, symbol: str, timeframe: str = "1", end_time: str = "") -> list[dict]:
        """해외 주식 당일 분봉 조회"""
        endpoint = "uapi/overseas-stock/v1/quotations/inquire-time-itemchartprice"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS76950200",
        }
        excd = "NAS" if self.exchange == "나스닥" else "NYS"
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symbol,
            "TM": end_time,
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json().get("output2", [])

    def fetch_investor_flow(self, symbol: str, end_time: str = "") -> list[dict]:
        """국내 주식 시간별 투자자 매매동향 조회 (10분 단위)"""
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-investor"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST01010900",
        }
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": symbol,
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json().get("output", [])

    def _fetch_balance_step(self, fk100: str = "", nk100: str = "") -> dict:
        """국내 잔고 또는 미체결 조회"""
        endpoint = "uapi/domestic-stock/v1/trading/inquire-balance"
        tr_id = "VTTC8434R" if self.is_simulation else "TTTC8434R"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }
        params = {
            "CANO": self.acc_no_prefix,
            "ACNT_PRDT_CD": self.acc_no_postfix,
            "AFHR_FLG": "N",
            "OFL_YN": "N",
            "INQR_DVSN": "01",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01", # 가공 처리된 잔고
            "CTX_AREA_FK100": fk100,
            "CTX_AREA_NK100": nk100,
        }
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        data = res.json()
        data["tr_cont"] = res.headers.get("tr_cont", "")
        return data

    def create_market_buy_order(self, symbol: str, quantity: int) -> dict:
        """시장가 매수 주문"""
        return self._create_domestic_order(symbol, quantity, 0, "03", "buy")

    def create_market_sell_order(self, symbol: str, quantity: int) -> dict:
        """시장가 매도 주문"""
        return self._create_domestic_order(symbol, quantity, 0, "03", "sell")

    def create_futureoption_buy_order(
        self,
        shtn_pdno: str,
        quantity: int,
        *,
        unit_price: int | float = 0,
        ord_dv: str = "day",
        ord_prcs_dvsn_cd: str = "02",
        nmpr_type_cd: str = "02",
        krx_nmpr_cndt_cd: str = "0",
        ord_dvsn_cd: str = "02",
        ctac_tlno: str = "",
        fuop_item_dvsn_cd: str = "",
        account_product_code: str | None = None,
    ) -> dict:
        """국내선물옵션 시장가/지정가 매수 주문"""
        return self._create_domestic_futureoption_order(
            shtn_pdno=shtn_pdno,
            quantity=quantity,
            side="buy",
            unit_price=unit_price,
            ord_dv=ord_dv,
            ord_prcs_dvsn_cd=ord_prcs_dvsn_cd,
            nmpr_type_cd=nmpr_type_cd,
            krx_nmpr_cndt_cd=krx_nmpr_cndt_cd,
            ord_dvsn_cd=ord_dvsn_cd,
            ctac_tlno=ctac_tlno,
            fuop_item_dvsn_cd=fuop_item_dvsn_cd,
            account_product_code=account_product_code,
        )

    def create_futureoption_sell_order(
        self,
        shtn_pdno: str,
        quantity: int,
        *,
        unit_price: int | float = 0,
        ord_dv: str = "day",
        ord_prcs_dvsn_cd: str = "02",
        nmpr_type_cd: str = "02",
        krx_nmpr_cndt_cd: str = "0",
        ord_dvsn_cd: str = "02",
        ctac_tlno: str = "",
        fuop_item_dvsn_cd: str = "",
        account_product_code: str | None = None,
    ) -> dict:
        """국내선물옵션 시장가/지정가 매도 주문"""
        return self._create_domestic_futureoption_order(
            shtn_pdno=shtn_pdno,
            quantity=quantity,
            side="sell",
            unit_price=unit_price,
            ord_dv=ord_dv,
            ord_prcs_dvsn_cd=ord_prcs_dvsn_cd,
            nmpr_type_cd=nmpr_type_cd,
            krx_nmpr_cndt_cd=krx_nmpr_cndt_cd,
            ord_dvsn_cd=ord_dvsn_cd,
            ctac_tlno=ctac_tlno,
            fuop_item_dvsn_cd=fuop_item_dvsn_cd,
            account_product_code=account_product_code,
        )

    def _create_domestic_order(self, symbol: str, quantity: int, price: int, order_type: str, side: str) -> dict:
        endpoint = "uapi/domestic-stock/v1/trading/order-cash"
        if side == "buy":
            tr_id = "VTTC0802U" if self.is_simulation else "TTTC0802U"
        else:
            tr_id = "VTTC0801U" if self.is_simulation else "TTTC0801U"

        body = {
            "CANO": self.acc_no_prefix,
            "ACNT_PRDT_CD": self.acc_no_postfix,
            "PDNO": symbol,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price),
        }
        request_url = f"{self.base_url}/{endpoint}"
        logger.info(
            "KIS order request profile={} simulation={} side={} symbol={} qty={} order_type={} tr_id={} url={}",
            self.credential_profile,
            self.is_simulation,
            side,
            symbol,
            quantity,
            order_type,
            tr_id,
            request_url,
        )
        data = self._request_with_auth("POST", endpoint, tr_id=tr_id, json=body)
        logger.info(
            "KIS order response side={} symbol={} qty={} status_code={} rt_cd={} msg_cd={} msg1={}",
            side,
            symbol,
            quantity,
            data.get("_http_status_code"),
            data.get("rt_cd"),
            data.get("msg_cd"),
            data.get("msg1"),
        )
        return data

    def _create_domestic_futureoption_order(
        self,
        *,
        shtn_pdno: str,
        quantity: int,
        side: str,
        unit_price: int | float = 0,
        ord_dv: str = "day",
        ord_prcs_dvsn_cd: str = "02",
        nmpr_type_cd: str = "02",
        krx_nmpr_cndt_cd: str = "0",
        ord_dvsn_cd: str = "02",
        ctac_tlno: str = "",
        fuop_item_dvsn_cd: str = "",
        account_product_code: str | None = None,
    ) -> dict:
        """국내선물옵션 주문"""
        if side not in {"buy", "sell"}:
            raise ValueError("side must be 'buy' or 'sell'")

        env_dv = "demo" if self.is_simulation else "real"
        if env_dv == "demo" and ord_dv != "day":
            raise ValueError("domestic futureoption demo orders only support ord_dv='day'")
        if ord_dv not in {"day", "night"}:
            raise ValueError("ord_dv can only be 'day' or 'night'")

        acnt_prdt_cd = (account_product_code or self.account_product_code or "").strip()
        if not acnt_prdt_cd:
            raise ValueError("Domestic futureoption orders require an account product code")
        if acnt_prdt_cd != "03":
            raise ValueError(
                f"Domestic futureoption orders require ACNT_PRDT_CD=03, got {acnt_prdt_cd!r}"
            )

        endpoint = "uapi/domestic-futureoption/v1/trading/order"
        sll_buy_dvsn_cd = "02" if side == "buy" else "01"
        body = {
            "ORD_PRCS_DVSN_CD": ord_prcs_dvsn_cd,
            "CANO": self.acc_no_prefix,
            "ACNT_PRDT_CD": acnt_prdt_cd,
            "SLL_BUY_DVSN_CD": sll_buy_dvsn_cd,
            "SHTN_PDNO": shtn_pdno,
            "ORD_QTY": str(quantity),
            "UNIT_PRICE": str(unit_price),
            "NMPR_TYPE_CD": nmpr_type_cd,
            "KRX_NMPR_CNDT_CD": krx_nmpr_cndt_cd,
            "ORD_DVSN_CD": ord_dvsn_cd,
            "CTAC_TLNO": ctac_tlno,
            "FUOP_ITEM_DVSN_CD": fuop_item_dvsn_cd,
        }

        tr_id = "VTTO1101U" if self.is_simulation else "TTTO1101U"
        request_url = f"{self.base_url}/{endpoint}"
        logger.info(
            "KIS futureoption order request profile={} simulation={} side={} shtn_pdno={} qty={} ord_dv={} tr_id={} url={}",
            self.credential_profile,
            self.is_simulation,
            side,
            shtn_pdno,
            quantity,
            ord_dv,
            tr_id,
            request_url,
        )
        data = self._request_with_auth(
            "POST",
            endpoint,
            tr_id=tr_id,
            json=body,
            use_hashkey=True,
        )
        logger.info(
            "KIS futureoption order response side={} shtn_pdno={} qty={} status_code={} rt_cd={} msg_cd={} msg1={}",
            side,
            shtn_pdno,
            quantity,
            data.get("_http_status_code"),
            data.get("rt_cd"),
            data.get("msg_cd"),
            data.get("msg1"),
        )
        return data

    def _build_headers(self, tr_id: str, token: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }

    def _issue_hashkey(self, payload: dict) -> str:
        endpoint = "uapi/hashkey"
        request_url = f"{self.base_url}/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
        }
        response = httpx.post(request_url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        data = response.json()
        hashkey = data.get("HASH") or data.get("hash")
        if not hashkey:
            raise RuntimeError(f"Failed to issue KIS hashkey: {data}")
        return str(hashkey)

    def _is_token_expired_response(self, data: dict) -> bool:
        message = str(data.get("msg1") or data.get("error_description") or "").lower()
        return "만료된 token" in message or "expired token" in message

    def _request_with_auth(
        self,
        method: str,
        endpoint: str,
        tr_id: str,
        params: dict | None = None,
        json: dict | None = None,
        use_hashkey: bool = False,
    ) -> dict:
        request_url = f"{self.base_url}/{endpoint}"

        for attempt in range(2):
            token = self.get_valid_token() if attempt == 0 else self.force_refresh_token()
            headers = self._build_headers(tr_id, token)
            if use_hashkey and json is not None:
                headers["hashkey"] = self._issue_hashkey(json)
            response = httpx.request(
                method,
                request_url,
                headers=headers,
                params=params,
                json=json,
                timeout=10,
            )
            data = response.json()
            data["_http_status_code"] = response.status_code

            if attempt == 0 and self._is_token_expired_response(data):
                logger.warning(
                    "KIS API reported expired token. Refreshing token and retrying once. endpoint={} tr_id={}",
                    endpoint,
                    tr_id,
                )
                continue

            return data

        return {"rt_cd": "1", "msg1": "토큰 재발급 후에도 요청이 실패했습니다."}

    # --- 하위 호환성 유지를 위한 Alias 메서드 ---
    def get_balance(self):
        return self.fetch_balance()

    def order_domestic_stock(self, code: str, quantity: int, price: int = 0, order_type: str = "01", side: str = "buy"):
        return self._create_domestic_order(code, quantity, price, order_type, side)

    def inquire_domestic_stock(self, code: str):
        return self.fetch_domestic_price(code)

    def inquire_oversee_stock(self, code: str):
        return self.fetch_oversea_price(code)


if __name__ == "__main__":
    import pprint
    handler = MarketHandler()

    # 현재가 테스트
    print("=== 삼성전자 현재가 ===")
    price_info = handler.fetch_price("005930")
    pprint.pprint(price_info)

    # 잔고 테스트
    print("\n=== 나의 전체 잔고 ===")
    balance_info = handler.fetch_balance()
    pprint.pprint(balance_info)

    # 해외주식 테스트
    print(f"\n=== AAPL (나스닥) 현재가 ===")
    handler_oversea = MarketHandler(exchange="나스닥")
    pprint.pprint(handler_oversea.fetch_price("AAPL"))

    # OHLCV 데이터 조회 테스트
    print("\n=== 삼성전자 최근 10일 일봉 ===")
    ohlcv = handler.fetch_ohlcv("005930", timeframe="D")
    for day in ohlcv[:10]:
        print(f"날짜: {day.get('stck_bsop_date')}, 종가: {day.get('stck_clpr')}, 변동: {day.get('prdy_vrss')}")

    print("\n=== TSLA 최근 10일 일봉 ===")
    tsla_ohlcv = handler_oversea.fetch_ohlcv("TSLA", timeframe="D")
    for day in tsla_ohlcv[:10]:
        print(f"날짜: {day.get('xymd')}, 종가: {day.get('clos')}, 변동: {day.get('diff')}")
