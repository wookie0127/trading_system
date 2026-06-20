from __future__ import annotations

from pathlib import Path

from trading_harness.agents.strategy_agent import StrategyAgent
from trading_harness.config import load_yaml, project_path
from trading_harness.data.repository import ParquetRepository
from trading_harness.report.markdown_reporter import MarkdownReporter


class ReportAgent:
    def __init__(
        self,
        ticker_config_path: str | Path = "configs/tickers.yaml",
        strategy_config_path: str | Path = "configs/strategies.yaml",
        backtest_config_path: str | Path = "configs/backtest.yaml",
    ) -> None:
        self.ticker_config = load_yaml(ticker_config_path)
        self.backtest_config = load_yaml(backtest_config_path)
        self.repository = ParquetRepository()
        self.strategy_agent = StrategyAgent(ticker_config_path, strategy_config_path)
        self.reporter = MarkdownReporter()

    def run(self) -> Path:
        features = self.repository.read(Path(self.ticker_config["data"]["features_dir"]) / "feature_dataset.parquet")
        signals = self.strategy_agent.load_signals()
        backtests = self.repository.read(self.backtest_config["backtest"]["output"])
        markdown = self.reporter.render(features, signals, backtests)
        output = project_path(Path(self.ticker_config["data"]["reports_dir"]) / "daily_trading_research_report.md")
        return self.reporter.write(markdown, output)
