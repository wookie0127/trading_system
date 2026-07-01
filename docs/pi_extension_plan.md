# Pi Extension Plan

This MVP keeps Pi integration loosely coupled. The Python harness should remain runnable without Pi, while Pi skills or extensions can call the same CLI entry points.

## Candidate Pi Skills or Prompt Templates

- Add a new strategy by implementing `BaseStrategy` and registering it in `StrategyAgent`.
- Interpret backtest results and identify drawdown, low trade count, and Sharpe degradation risks.
- Analyze why strategy performance changed across recent 30-day and 90-day windows.
- Suggest new features from macro, volatility, crypto, and Korea market context.
- Summarize the daily Markdown report for Slack or Telegram.

## Candidate TypeScript Extension Commands

- `run_backtest`: execute `python scripts/run_backtest.py`.
- `generate_report`: execute `python scripts/run_report.py`.
- `inspect_strategy`: read a strategy config and recent signal parquet.
- `compare_strategies`: compare `data/backtests/backtest_result.parquet` across strategies.

## Integration Principle

Pi should call stable CLI commands or read stable parquet and Markdown artifacts. It should not own trading state, broker credentials, or live execution logic.
