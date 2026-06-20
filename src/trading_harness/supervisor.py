from __future__ import annotations

from trading_harness.agents.backtest_agent import BacktestAgent
from trading_harness.agents.feature_agent import FeatureAgent
from trading_harness.agents.market_data_agent import MarketDataAgent
from trading_harness.agents.report_agent import ReportAgent
from trading_harness.agents.strategy_agent import StrategyAgent


class TradingResearchSupervisor:
    def run(self) -> dict[str, object]:
        outputs: dict[str, object] = {}
        outputs["market_data"] = MarketDataAgent().run()
        outputs["features"] = FeatureAgent().run()
        outputs["signals"] = StrategyAgent().run()
        outputs["backtests"] = BacktestAgent().run()
        outputs["report"] = ReportAgent().run()
        return outputs


def main() -> None:
    outputs = TradingResearchSupervisor().run()
    for step, output in outputs.items():
        print(f"{step}: {output}")


if __name__ == "__main__":
    main()
