from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from strategies.base import BaseStrategy
from strategies.parameters import StrategyParameter
from strategies.moving_average import MovingAverageCrossStrategy
from strategies.ma_goldencross import MovingAverageGoldenCrossStrategy

StrategyStatus = Literal["stable", "experimental", "deprecated"]

@dataclass(frozen=True)
class StrategySpec:
    key: str
    name: str
    strategy_class: type[BaseStrategy]
    parameters: tuple[StrategyParameter, ...]
    status: StrategyStatus = "experimental"
    description: str | None = None


STRATEGY_REGISTRY: dict[str, StrategySpec] = {
    "moving_average_cross": StrategySpec(
        key="moving_average_cross",
        name="Moving Average Cross",
        strategy_class=MovingAverageCrossStrategy,
        parameters=(
            StrategyParameter(
                key="fast_window",
                label="Fast MA",
                default=20,
                min_value=2,
                max_value=240,
                step=1,
                value_type=int
            ),
            StrategyParameter(
                key="slow_window",
                label="Slow MA",
                default=60,
                min_value=3,
                max_value=480,
                step=1,
                value_type=int
            ),
        ),
        status="experimental",
        description="A strategy that generates buy/sell signals based on the crossover of two moving averages (fast and slow).",
    ),
    "ma_goldencross": StrategySpec(
        key="ma_goldencross",
        name="Moving Average Golden Cross",
        strategy_class=MovingAverageGoldenCrossStrategy,
        parameters=(
            StrategyParameter(
                key="short_ma_period",
                label="Short MA Period",
                default=20,
                min_value=1,
                max_value=240,
                step=1,
                value_type=int
            ),
            StrategyParameter(
                key="long_ma_period",
                label="Long MA Period",
                default=60,
                min_value=2,
                max_value=480,
                step=1,
                value_type=int
            ),
        ),
        status="experimental",
        description="A strategy that generates buy/sell signals based on the golden cross and death cross of two moving averages (short and long).",
    ),
}


def get_strategy_spec(key: str) -> StrategySpec:
    try:
        return STRATEGY_REGISTRY[key]
    except KeyError as exc:
        raise ValueError(f"Unknown strategy: {key}") from exc


def instantiate_strategy(spec: StrategySpec, params: dict[str, Any]) -> BaseStrategy:
    return spec.strategy_class(**params)
