import requests
import time
import datetime
import pandas as pd
from pathlib import Path

DATA_DIR = Path("./minute_data")
DATA_DIR.mkdir(exist_ok=True)

symbols = ["BTCUSDT", "ETHUSDT"]
interval = "1m"


def get_latest_1m_kline(symbol):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": symbol, "interval": interval, "limit": 1}
    response = requests.get(url, params=params)
    response.raise_for_status()
    data = response.json()
    return data[0]


def main():
    collected = {sym: [] for sym in symbols}
    current_day = datetime.datetime.now().date()

    while True:
        try:
            # 다음날이 되면 저장 후 초기화
            now_day = datetime.datetime.now().date()
            if now_day > current_day:
                for sym in symbols:
                    if collected[sym]:
                        df = pd.DataFrame(
                            collected[sym],
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
                        out_path = DATA_DIR / f"{sym}_{current_day}.csv"
                        df.to_csv(out_path, index=False)
                        print(f"Saved {len(df)} rows to {out_path}")
                        collected[sym] = []
                current_day = now_day

            for sym in symbols:
                try:
                    kline = get_latest_1m_kline(sym)
                    kline_time = datetime.datetime.fromtimestamp(kline[0] // 1000)
                    collected[sym].append(kline)
                    print(f"Collected {sym} {kline_time} data")
                except Exception as e:
                    print(f"Error collecting {sym}: {e}")

            time.sleep(60)  # 정확한 분 간격을 원한다면 time sync 필요

        except Exception as e:
            print("Error in main loop:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
