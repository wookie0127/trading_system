# Backtest Assumptions

The MVP backtest engine is intentionally simple and deterministic.

## Execution Model

- Long-only.
- `signal = 1` means hold the asset.
- `signal = 0` means hold cash.
- Position uses the prior row's signal to avoid same-close lookahead.
- Position size is 100% of capital for each independent symbol backtest.

## Costs

Default values:

- `fee_rate = 0.0005`
- `slippage_rate = 0.0005`

The engine subtracts total transaction cost when position changes.

## Metrics

- `total_return`
- `annualized_return`
- `sharpe_ratio`
- `max_drawdown`
- `win_rate`
- `number_of_trades`
- `average_trade_return`
- `exposure`

## Exclusions

- No live orders.
- No short selling.
- No leverage.
- No intraday fill modeling.
- No market impact model.
- No broker-specific tax or fee schedule.
