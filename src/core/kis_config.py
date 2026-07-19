try:
    from src.core.config import DATA_DIR, DATA_REFERENCE_DIR
except ImportError:
    from core.config import DATA_DIR, DATA_REFERENCE_DIR

DATADIR = DATA_DIR
PROJECT_DATA_DIR = DATA_DIR

CODE_PATH_BOOK = {
    "nasdaq": DATA_REFERENCE_DIR / "nasdaq100_symbols.json",
    "kospi": DATA_REFERENCE_DIR / "kospi200_symbols.json",
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
