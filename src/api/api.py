import sys as _sys; from pathlib import Path as _Path
_sys.path.insert(0, str(_Path(__file__).parents[1]))  # src/ 패키지 루트
del _sys, _Path

from fastapi import FastAPI, HTTPException
from core.kis_market_handler import MarketHandler
import os

app = FastAPI()
market_handler = MarketHandler()


@app.get("/")
def read_root():
    return {"message": "Welcome to the trading system API!"}


@app.post("/buy/{symbol}/{quantity}")
def buy_stock(symbol: str, quantity: int):
    # Try to get code if symbol is a name
    code = market_handler.get_code(symbol) or symbol
    try:
        res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="buy", order_type="03")
        if res.get("rt_cd") == "0":
            return {"message": f"Bought {quantity} shares of {symbol}", "data": res}
        else:
            raise HTTPException(status_code=400, detail=res.get("msg1"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/sell/{symbol}/{quantity}")
def sell_stock(symbol: str, quantity: int):
    code = market_handler.get_code(symbol) or symbol
    try:
        res = market_handler.order_domestic_stock(code=code, quantity=quantity, side="sell", order_type="03")
        if res.get("rt_cd") == "0":
            return {"message": f"Sold {quantity} shares of {symbol}", "data": res}
        else:
            raise HTTPException(status_code=400, detail=res.get("msg1"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/balance")
def get_balance():
    try:
        return market_handler.get_balance()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    """시스템 및 KIS API 상태 체크"""
    try:
        token = market_handler.get_valid_token()
        return {
            "status": "healthy",
            "kis_auth": "connected" if token else "disconnected",
            "cano_set": bool(os.getenv("KIS_CANO"))
        }
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
