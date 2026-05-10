from pathlib import Path

DATADIR = Path(__file__).parents[1] / "data"
PROJECT_DATA_DIR = Path(__file__).parents[2] / "data"

CODE_PATH_BOOK = {
    "nasdaq": PROJECT_DATA_DIR / "reference" / "nasdaq100_symbols.json",
    "kospi": PROJECT_DATA_DIR / "reference" / "kospi200_symbols.json",
}

API_ROOT = "https://openapi.koreainvestment.com:9443"

STOCK_WHERE = {
    "국내": "domestic-stock",
    "해외": "overseas-stock",
}

ENDPOINT_BOOK = {
    "order-cash": "uapi/{where}/v1/trading/order-cash",
    "order-rvsecncl": "uapi/{where}/v1/trading/order-rvsecncl",
    "inqure-psbl-order": "uapi/{where}/v1/trading/inquire-psbl-order",
    "inquire-balance": "uapi/{where}/v1/trading/inquire-balance",
    "search": "uapi/{where}/v1/quotations/inquire-search",
}

URL_TOKEN = f"{API_ROOT}/oauth2/tokenP"
