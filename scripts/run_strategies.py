from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_harness.agents.strategy_agent import StrategyAgent


if __name__ == "__main__":
    for name, output in StrategyAgent().run().items():
        print(f"{name}: {output}")
