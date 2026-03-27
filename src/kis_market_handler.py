import json
import os
import time
from pathlib import Path

import httpx
import yaml
from dotenv import load_dotenv

from kis_auth_handler import KISAuthHandler
from kis_config import CODE_PATH_BOOK
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
        return json.load(f)


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
        self._code_df = load_codes()
        self.exchange = exchange

        # 계좌 설정 (8자리-2자리 분리)
        cano = os.getenv("KIS_CANO", "")
        prdt_cd = os.getenv("KIS_ACNT_PRDT_CD", "01")

        if "-" in cano:
            self.acc_no_prefix = cano.split("-")[0]
            self.acc_no_postfix = cano.split("-")[1]
        else:
            self.acc_no_prefix = cano
            self.acc_no_postfix = prdt_cd.zfill(2)

    def get_code(self, company_name: str) -> str | None:
        """종목명으로 종목 코드 찾기"""
        for stock in self._code_df:
            if stock.get("ko_name") == company_name or stock.get("en_name") == company_name:
                return stock.get("code")

        logger.warning(f"No code found for company: {company_name}")
        return None

    def fetch_price(self, symbol: str) -> dict:
        """현재가 조회 (국내/해외 통합)"""
        if self.exchange == "서울":
            return self.fetch_domestic_price(symbol)
        else:
            return self.fetch_oversea_price(symbol)

    def fetch_domestic_price(self, symbol: str) -> dict:
        """국내 주식 현재가 조회"""
        endpoint = "uapi/domestic-stock/v1/quotations/inquire-price"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "FHKST01010100",
        }
        params = {"fid_cond_mrkt_div_code": "J", "fid_input_iscd": symbol}
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json()

    def fetch_oversea_price(self, symbol: str) -> dict:
        """해외 주식 현재가 조회"""
        endpoint = "uapi/overseas-price/v1/quotations/price"
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": "HHDFS00000300",
        }
        # KIS 해외 거래소 코드 반영 (나스닥: NAS, 뉴욕: NYS)
        excd = "NAS" if self.exchange == "나스닥" else "NYS"
        params = {"AUTH": "", "EXCD": excd, "SYMB": symbol}
        res = httpx.get(f"{self.base_url}/{endpoint}", headers=headers, params=params, timeout=10)
        return res.json()

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

    def _create_domestic_order(self, symbol: str, quantity: int, price: int, order_type: str, side: str) -> dict:
        endpoint = "uapi/domestic-stock/v1/trading/order-cash"
        if side == "buy":
            tr_id = "VTTC0802U" if self.is_simulation else "TTTC0802U"
        else:
            tr_id = "VTTC0801U" if self.is_simulation else "TTTC0801U"

        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.get_valid_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
        }
        body = {
            "CANO": self.acc_no_prefix,
            "ACNT_PRDT_CD": self.acc_no_postfix,
            "PDNO": symbol,
            "ORD_DVSN": order_type,
            "ORD_QTY": str(quantity),
            "ORD_UNPR": str(price),
        }
        res = httpx.post(f"{self.base_url}/{endpoint}", headers=headers, json=body, timeout=10)
        return res.json()

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
