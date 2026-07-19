from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class PriceInfo(BaseModel):
    close: float
    return_4h: float
    return_24h: float
    range_4h: float


class HigherTimeframeInfo(BaseModel):
    daily_trend: str  # "UP", "DOWN", "SIDEWAYS"
    price_vs_ema_200: float


class TechnicalInfo(BaseModel):
    ema_20: float
    ema_50: float
    ema_100: float
    ema_200: float
    rsi_14: float
    macd_histogram: float
    atr_ratio: float
    bollinger_position: float
    volume_zscore: float


class DerivativesInfo(BaseModel):
    funding_rate: float = 0.0
    open_interest_change_4h: float = 0.0
    long_liquidation_4h: float = 0.0


class SentimentInfo(BaseModel):
    fear_greed_index: int = 50


class PositionInfo(BaseModel):
    status: str = "FLAT"  # "FLAT", "LONG", "SHORT"
    side: Optional[str] = None
    entry_price: Optional[float] = None
    unrealized_pnl_ratio: float = 0.0


class PreviousDecisionInfo(BaseModel):
    action: str = "NO_TRADE"
    thesis: str = ""
    invalidation_conditions: List[str] = Field(default_factory=list)


class DataQualityInfo(BaseModel):
    is_complete: bool = True
    stale_sources: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)


class MarketSnapshot(BaseModel):
    run_at: str
    symbol: str = "BTCUSDT"
    decision_timeframe: str = "4h"
    price: PriceInfo
    higher_timeframe: HigherTimeframeInfo
    technical: TechnicalInfo
    derivatives: DerivativesInfo = Field(default_factory=DerivativesInfo)
    sentiment: SentimentInfo = Field(default_factory=SentimentInfo)
    position: PositionInfo = Field(default_factory=PositionInfo)
    previous_decision: PreviousDecisionInfo = Field(default_factory=PreviousDecisionInfo)
    data_quality: DataQualityInfo = Field(default_factory=DataQualityInfo)
