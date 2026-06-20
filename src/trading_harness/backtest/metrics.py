from __future__ import annotations

import math
from statistics import fmean, pstdev


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        worst = min(worst, value / peak - 1)
    return float(worst)


def summarize_returns(
    daily_returns: list[float],
    positions: list[int],
    trade_returns: list[float],
    trading_days_per_year: int = 252,
) -> dict[str, float | int]:
    cleaned_returns = [0.0 if value is None else float(value) for value in daily_returns]
    equity: list[float] = []
    current = 1.0
    for value in cleaned_returns:
        current *= 1 + value
        equity.append(current)

    total_return = equity[-1] - 1 if equity else 0.0
    years = max(len(cleaned_returns) / trading_days_per_year, 1 / trading_days_per_year)
    annualized_return = (1 + total_return) ** (1 / years) - 1
    std = pstdev(cleaned_returns) if len(cleaned_returns) > 1 else 0.0
    sharpe_ratio = math.sqrt(trading_days_per_year) * fmean(cleaned_returns) / std if std else 0.0
    number_of_trades = len(trade_returns)
    wins = sum(1 for value in trade_returns if value > 0)
    return {
        "total_return": float(total_return),
        "annualized_return": float(annualized_return),
        "sharpe_ratio": float(sharpe_ratio),
        "max_drawdown": max_drawdown(equity),
        "win_rate": wins / number_of_trades if number_of_trades else 0.0,
        "number_of_trades": number_of_trades,
        "average_trade_return": fmean(trade_returns) if number_of_trades else 0.0,
        "exposure": fmean(positions) if positions else 0.0,
    }
