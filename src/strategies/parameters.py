from dataclasses import dataclass


@dataclass(frozen=True)
class StrategyParameter:
    key: str
    label: str
    value_type: type
    default: int | float | str | bool
    min_value: int | float | None = None
    max_value: int | float | None = None
    step: int | float | None = None
    format: str | None = None
