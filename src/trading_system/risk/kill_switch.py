class KillSwitch:
    def __init__(
        self,
        max_daily_loss_ratio: float = 0.02,
        max_drawdown_ratio: float = 0.08,
        max_consecutive_losses: int = 4,
    ):
        self.max_daily_loss_ratio = max_daily_loss_ratio
        self.max_drawdown_ratio = max_drawdown_ratio
        self.max_consecutive_losses = max_consecutive_losses
        self.is_active = False
        self.active_reason = ""

    def check(
        self,
        daily_loss_ratio: float,
        drawdown_ratio: float,
        consecutive_losses: int,
        data_is_stale: bool = False,
    ) -> bool:
        """
        Triggers kill switch if risk limits are violated or data is stale.
        Returns True if KillSwitch is ACTIVATED (trading halted).
        """
        if data_is_stale:
            self.is_active = True
            self.active_reason = "Stale data detected"
            return True

        if daily_loss_ratio >= self.max_daily_loss_ratio:
            self.is_active = True
            self.active_reason = f"Daily loss limit exceeded: {daily_loss_ratio:.2%} >= {self.max_daily_loss_ratio:.2%}"
            return True

        if drawdown_ratio >= self.max_drawdown_ratio:
            self.is_active = True
            self.active_reason = f"Max drawdown limit exceeded: {drawdown_ratio:.2%} >= {self.max_drawdown_ratio:.2%}"
            return True

        if consecutive_losses >= self.max_consecutive_losses:
            self.is_active = True
            self.active_reason = f"Max consecutive losses reached: {consecutive_losses} >= {self.max_consecutive_losses}"
            return True

        return False
