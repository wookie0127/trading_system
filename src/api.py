from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return {"message": "Welcome to the trading system API!"}


@app.post("/buy/{symbol}/{quantity}")
def buy_stock(symbol: str, quantity: int):
    # 여기에 매수 로직을 구현합니다.
    # 예를 들어, 주문을 실행하고 성공 여부를 반환할 수 있습니다.
    return {"message": f"Bought {quantity} shares of {symbol}"}


@app.post("/sell/{symbol}/{quantity}")
def sell_stock(symbol: str, quantity: int):
    # 여기에 매도 로직을 구현합니다.
    # 예를 들어, 주문을 실행하고 성공 여부를 반환할 수 있습니다.
    return {"message": f"Sold {quantity} shares of {symbol}"}
