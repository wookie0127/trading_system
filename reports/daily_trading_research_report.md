# Daily Trading Research Report

## 1. Market Summary
- Latest data date: 2026-06-29
- Recent 30 rows return leaders: usdkrw: 3.07%, qqq: -1.15%, vix: -1.30%
- Recent 90 rows return leaders: qqq: 17.50%, nasdaq: 12.04%, usdkrw: 6.90%

## 2. Strategy Signals
- trend_following: active=none
- us_market_shock_inverse: active=none

## 3. Backtest Results
| strategy | symbol | total_return | annualized_return | sharpe_ratio | max_drawdown | win_rate | number_of_trades | average_trade_return |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| us_market_shock_inverse | kospi200_inverse | 3.0285 | 0.2512 | 2.1937 | -0.0453 | 0.1120 | 125 | 0.0018 |
| trend_following | qqq | 0.2110 | 0.0301 | 0.3045 | -0.2299 | 0.2687 | 67 | 0.0034 |
| trend_following | btc | 5.4149 | 0.2183 | 0.8582 | -0.3915 | 0.3067 | 75 | 0.0336 |
| trend_following | eth | 5.0409 | 0.2106 | 0.6951 | -0.4746 | 0.2533 | 75 | 0.0270 |

## 4. Risk Notes
- This MVP is research-only and does not place live orders.
- Backtests use close-to-close returns with fixed fee and slippage assumptions.
- Korea inverse execution is represented as a signal until matching local market data is added.

## 5. Next Actions
- Review strategies with negative drawdown and low trade count before adding capital assumptions.
- Add Korea inverse ETF price history for executable shock-signal backtests.
- Add Slack or Telegram delivery behind an explicit notification command.
