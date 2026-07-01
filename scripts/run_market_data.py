from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_harness.agents.market_data_agent import MarketDataAgent


if __name__ == "__main__":
    for output in MarketDataAgent().run():
        print(output)
