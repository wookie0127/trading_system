from typing import List, Literal, Optional
from pydantic import BaseModel, Field


class Evidence(BaseModel):
    category: Literal[
        "technical",
        "derivatives",
        "macro",
        "news",
        "position",
    ]
    description: str


class TradingDecision(BaseModel):
    regime: Literal[
        "TREND_UP",
        "TREND_DOWN",
        "RANGE",
        "HIGH_VOLATILITY",
        "UNCERTAIN",
    ]

    strategy: Literal[
        "TREND_FOLLOWING",
        "MEAN_REVERSION",
        "BREAKOUT",
        "NONE",
    ]

    action: Literal[
        "OPEN_LONG",
        "OPEN_SHORT",
        "HOLD",
        "REDUCE",
        "CLOSE",
        "NO_TRADE",
    ]

    confidence: float = Field(ge=0.0, le=1.0)

    thesis: str
    supporting_evidence: List[Evidence]
    contradicting_evidence: List[Evidence]
    invalidation_conditions: List[str]
    alternative_scenario: str
    risk_flags: List[str]

    risk_profile: Literal[
        "CONSERVATIVE",
        "BALANCED",
        "AGGRESSIVE",
    ]
