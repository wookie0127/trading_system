from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from trading_harness.agents.report_agent import ReportAgent


if __name__ == "__main__":
    print(ReportAgent().run())
