# Quant Trading Project Roadmap

## 1. Conversation Summary

### Core Principle

The minimum unit of backtesting is:

    Single Asset × Single Timeframe × Single Strategy

Examples:

-   Samsung Electronics + 1m + MA20/40
-   SK Hynix + 1m + MA20/40
-   BTCUSDT + 1m + MA20/40

Portfolio allocation and multi-asset optimization are later-stage
problems.

------------------------------------------------------------------------

## 2. Risk Management

### Stop Loss and Take Profit are mandatory

Recommended exit conditions:

-   Signal Exit
-   Stop Loss
-   Take Profit

Example:

-   Stop Loss: -2%
-   Take Profit: +4%

or

-   Stop Loss: 2 × ATR
-   Take Profit: 4 × ATR

------------------------------------------------------------------------

## 3. Machine Learning Strategy

Avoid:

    OHLCV -> Deep Learning -> Buy/Sell

Prefer:

    Strategy
    ↓
    Parameter Search
    ↓
    Optimization

Candidate parameters:

-   Fast MA
-   Slow MA
-   Stop Loss
-   Take Profit

Recommended library:

-   Optuna

------------------------------------------------------------------------

## 4. Alpha Discovery Philosophy

Universal alpha across all markets is unlikely.

Instead:

-   BTC
-   ETH
-   Samsung Electronics
-   SK Hynix
-   Nasdaq Futures

Each asset has its own market microstructure and participant behavior.

------------------------------------------------------------------------

## 5. Trade Lifecycle

    Entry
    ↓
    Exit
    ↓
    Trade Return Calculation
    ↓
    Accumulation

Trade return:

    (exit_price - entry_price) / entry_price

------------------------------------------------------------------------

# Development Roadmap

## Phase 1 --- Data Collection

### Goal

Build stable historical and realtime pipelines.

### Required Timeframes

-   1m
-   5m
-   15m
-   30m
-   1h
-   4h
-   1d

### Initial Target Asset

Choose one:

-   Samsung Electronics
-   SK Hynix
-   BTCUSDT

BTCUSDT is recommended because of:

-   Free data
-   24/7 market
-   High liquidity

------------------------------------------------------------------------

## Phase 2 --- Backtest Engine

### Required Components

#### Position State

-   LONG
-   SHORT
-   FLAT

#### Entry Logic

-   Buy Signal

#### Exit Logic

-   Sell Signal
-   Stop Loss
-   Take Profit

#### Cost Model

-   Trading Fee
-   Slippage
-   Tax

#### Metrics

-   CAGR
-   Sharpe Ratio
-   Maximum Drawdown
-   Profit Factor
-   Win Rate
-   Number of Trades
-   Average Holding Time

------------------------------------------------------------------------

## Phase 3 --- Strategy Research

Candidate strategies:

-   Moving Average Cross
-   RSI
-   MACD
-   Bollinger Bands

------------------------------------------------------------------------

## Phase 4 --- Hyperparameter Optimization

Optimize:

-   MA Fast Window
-   MA Slow Window
-   Stop Loss
-   Take Profit

Recommended framework:

-   Optuna

------------------------------------------------------------------------

## Phase 5 --- Walk Forward Validation

Examples:

    Train: 2024
    Test: 2025

or

    Train: Previous 6 months
    Test: Next 1 month

------------------------------------------------------------------------

## Phase 6 --- Portfolio Construction

Only after strategy validation:

Examples:

-   Samsung Electronics 30%
-   SK Hynix 40%
-   Hyundai Motor 30%

------------------------------------------------------------------------

# Immediate TODO

## This Week

### 1. Select One Asset

Recommended:

-   BTCUSDT
-   Samsung Electronics

### 2. Improve Backtest Engine

Add:

-   Stop Loss
-   Take Profit
-   Trading Fee
-   Slippage

### 3. Add Evaluation Metrics

-   CAGR
-   Sharpe Ratio
-   MDD
-   Profit Factor

### 4. Generate Multi-Timeframe Data

Create:

-   5m
-   15m
-   30m
-   1h
-   4h

from 1m bars.

### 5. Validate MA20/40 Strategy

Questions to answer:

-   Number of trades?
-   Win rate?
-   Maximum drawdown?
-   Sharpe ratio?
-   Excess return over benchmark?

------------------------------------------------------------------------

# Guidance for Codex

Priority order:

1.  Data Pipeline
2.  Backtest Engine
3.  Backtest Validation
4.  Strategy Research
5.  Parameter Optimization
6.  Portfolio Management

If the equity curve looks too good too early:

> Assume the backtest engine is wrong before assuming the strategy is
> excellent.
