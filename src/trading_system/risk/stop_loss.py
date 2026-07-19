class StopLossCalculator:
    @staticmethod
    def calculate_stop_loss(
        side: str,  # "LONG" or "SHORT"
        entry_price: float,
        atr_14: float,
        multiplier: float = 2.0
    ) -> float:
        """
        Calculates deterministic stop loss price based on ATR multiplier.
        """
        if atr_14 <= 0:
            atr_14 = entry_price * 0.02  # Fallback 2% if ATR is missing

        distance = atr_14 * multiplier
        if side == "LONG":
            return round(entry_price - distance, 2)
        elif side == "SHORT":
            return round(entry_price + distance, 2)
        return entry_price
