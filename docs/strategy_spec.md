# Strategy Specifications

## Strategy A: US Market Shock to Korea Inverse Signal

Name: `us_market_shock_inverse`

Signal rule:

```text
QQQ return_1d <= -1.5%
AND VIX return_1d >= 5%
THEN signal = 1 for kospi200_inverse
```

Output:

```text
data/signals/us_market_shock_inverse_signal.parquet
```

The MVP generates signals only. It does not execute Korea market trades.

## Strategy B: Simple Trend Following

Name: `trend_following`

Universe:

- QQQ
- BTC
- ETH

Signal rule:

```text
close > ma_20
AND ma_20 > ma_60
THEN signal = 1
ELSE signal = 0
```

Output:

```text
data/signals/trend_following_signal.parquet
```
