class PositionSizer:
    @staticmethod
    def calculate_position_size(
        account_balance: float,
        entry_price: float,
        stop_loss_price: float,
        max_risk_ratio: float = 0.005,
        max_position_ratio: float = 0.15,
        max_leverage: float = 2.0,
    ) -> float:
        """
        Deterministic Position Sizing Formula:
        Allowed Risk ($) = Account Balance * max_risk_ratio
        Stop Distance ($) = abs(entry_price - stop_loss_price)
        Raw Size (BTC) = Allowed Risk / Stop Distance
        Cap by max_position_ratio and max_leverage.
        """
        if account_balance <= 0 or entry_price <= 0 or stop_loss_price <= 0:
            return 0.0

        stop_distance = abs(entry_price - stop_loss_price)
        if stop_distance <= 0:
            return 0.0

        allowed_risk_usd = account_balance * max_risk_ratio
        size_by_risk = allowed_risk_usd / stop_distance

        # Capital Capping
        max_position_usd = account_balance * max_position_ratio * max_leverage
        max_size = max_position_usd / entry_price

        final_size = min(size_by_risk, max_size)
        return round(final_size, 4)
