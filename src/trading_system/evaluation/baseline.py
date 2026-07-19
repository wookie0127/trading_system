from typing import Literal


class BaselineStrategies:
    @staticmethod
    def rsi_mean_reversion(
        rsi_14: float, bollinger_pos: float
    ) -> Literal["OPEN_LONG", "OPEN_SHORT", "NO_TRADE"]:
        if rsi_14 < 30 and bollinger_pos < -1.0:
            return "OPEN_LONG"
        elif rsi_14 > 70 and bollinger_pos > 1.0:
            return "OPEN_SHORT"
        return "NO_TRADE"

    @staticmethod
    def ema_trend_following(
        close: float, ema_20: float, ema_50: float, ema_200: float
    ) -> Literal["OPEN_LONG", "OPEN_SHORT", "NO_TRADE"]:
        if close > ema_200 and ema_20 > ema_50:
            return "OPEN_LONG"
        elif close < ema_200 and ema_20 < ema_50:
            return "OPEN_SHORT"
        return "NO_TRADE"
