import requests
import time
import datetime
import pandas as pd
from pathlib import Path

DATA_DIR = Path("./minute_data")
DATA_DIR.mkdir(exist_ok=True)

symbol = "BTCUSDT"
interval = "1m"


def get_latest_1m_kline(symbol):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": 1}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data[0]


def main():
    collected = []
    current_day = datetime.datetime.now().date()

    while True:
        try:
            kline = get_latest_1m_kline(symbol)
            kline_time = datetime.datetime.fromtimestamp(kline[0] // 1000)
            kline_day = kline_time.date()

            # 다음날이 되면 저장 후 초기화
            if kline_day > current_day:
                df = pd.DataFrame(
                    collected,
                    columns=[
                        "open_time",
                        "open",
                        "high",
                        "low",
                        "close",
                        "volume",
                        "close_time",
                        "quote_asset_volume",
                        "num_trades",
                        "taker_buy_base_volume",
                        "taker_buy_quote_volume",
                        "ignore",
                    ],
                )
                df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
                out_path = DATA_DIR / f"{symbol}_{current_day}.csv"
                df.to_csv(out_path, index=False)
                print(f"Saved {len(df)} rows to {out_path}")

                collected = []
                current_day = kline_day

            collected.append(kline)
            print(f"Collected {kline_time} data")
            time.sleep(60)  # 정확한 분 간격을 원한다면 time sync 필요

        except Exception as e:
            print("Error:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
