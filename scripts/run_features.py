from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_harness.agents.feature_agent import FeatureAgent


if __name__ == "__main__":
    print(FeatureAgent().run())
