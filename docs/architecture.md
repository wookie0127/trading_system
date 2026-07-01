# Trading Research Harness Architecture

The harness is a research-only pipeline for repeatable market data collection, feature generation, strategy signal creation, backtesting, and Markdown reporting.

## Components

- `MarketDataAgent`: loads cached local parquet data or downloads daily OHLCV data through yfinance.
- `FeatureAgent`: builds technical features from normalized raw OHLCV data.
- `StrategyAgent`: runs independent strategy implementations and writes signal parquet files.
- `BacktestAgent`: runs a simple long-only signal backtest and writes strategy metrics.
- `ReportAgent`: renders deterministic Markdown output from feature, signal, and backtest artifacts.
- `TradingResearchSupervisor`: orchestrates the full agent sequence.

## Data Flow

```text
configs/*.yaml
  -> MarketDataAgent
  -> data/raw/*.parquet
  -> FeatureAgent
  -> data/features/feature_dataset.parquet
  -> StrategyAgent
  -> data/signals/*.parquet
  -> BacktestAgent
  -> data/backtests/backtest_result.parquet
  -> ReportAgent
  -> reports/daily_trading_research_report.md
```

## Data Engine

- In-memory transformations use `polars`.
- Local parquet/csv reads go through DuckDB in `ParquetRepository`.
- Writes use Polars parquet output.
- `yfinance` may return pandas objects at the API boundary, but those frames are immediately normalized into Polars frames.

## Boundary Decisions

- Live orders are explicitly excluded.
- Broker integration should be added behind interfaces and explicit commands only.
- Notification delivery should be added behind explicit Slack or Telegram commands.
- Strategy logic and backtest execution are separate modules.
- Paths and operational assumptions live in config files.
