from datetime import datetime, timezone
import pandas as pd
from typing import Optional, Dict, Any

from src.trading_system.features.technical import compute_technical_indicators
from src.trading_system.snapshots.schemas import (
    MarketSnapshot, PriceInfo, HigherTimeframeInfo, TechnicalInfo,
    DerivativesInfo, SentimentInfo, PositionInfo, PreviousDecisionInfo, DataQualityInfo
)


class SnapshotBuilder:
    def __init__(self):
        pass

    def build_snapshot_from_df(
        self,
        df_4h: pd.DataFrame,
        current_idx: int,
        current_position: Optional[PositionInfo] = None,
        previous_decision: Optional[PreviousDecisionInfo] = None
    ) -> MarketSnapshot:
        """
        Builds a point-in-time MarketSnapshot from df_4h sliced up to current_idx (inclusive).
        Prevents look-ahead bias by only looking at rows <= current_idx.
        """
        sub_df = df_4h.iloc[:current_idx + 1].copy()
        sub_df = compute_technical_indicators(sub_df)
        
        curr_row = sub_df.iloc[-1]
        prev_row = sub_df.iloc[-2] if len(sub_df) > 1 else curr_row
        row_24h = sub_df.iloc[-7] if len(sub_df) >= 7 else sub_df.iloc[0]

        close_price = float(curr_row['close'])
        prev_close = float(prev_row['close'])
        close_24h = float(row_24h['close'])

        return_4h = (close_price - prev_close) / prev_close if prev_close else 0.0
        return_24h = (close_price - close_24h) / close_24h if close_24h else 0.0
        range_4h = (float(curr_row['high']) - float(curr_row['low'])) / close_price if close_price else 0.0

        ema_200 = float(curr_row['ema_200'])
        price_vs_ema_200 = (close_price - ema_200) / ema_200 if ema_200 else 0.0
        daily_trend = "UP" if price_vs_ema_200 > 0 else "DOWN"

        run_at_str = str(curr_row['timestamp'])

        snapshot = MarketSnapshot(
            run_at=run_at_str,
            symbol="BTCUSDT",
            decision_timeframe="4h",
            price=PriceInfo(
                close=close_price,
                return_4h=return_4h,
                return_24h=return_24h,
                range_4h=range_4h
            ),
            higher_timeframe=HigherTimeframeInfo(
                daily_trend=daily_trend,
                price_vs_ema_200=price_vs_ema_200
            ),
            technical=TechnicalInfo(
                ema_20=float(curr_row['ema_20']),
                ema_50=float(curr_row['ema_50']),
                ema_100=float(curr_row['ema_100']),
                ema_200=ema_200,
                rsi_14=float(curr_row['rsi_14']),
                macd_histogram=float(curr_row['macd_histogram']),
                atr_ratio=float(curr_row['atr_ratio']),
                bollinger_position=float(curr_row['bollinger_position']),
                volume_zscore=float(curr_row['volume_zscore'])
            ),
            derivatives=DerivativesInfo(),
            sentiment=SentimentInfo(),
            position=current_position or PositionInfo(),
            previous_decision=previous_decision or PreviousDecisionInfo(),
            data_quality=DataQualityInfo(is_complete=True)
        )
        return snapshot
