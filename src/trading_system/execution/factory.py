from typing import Dict, Any, Optional
from src.trading_system.execution.base import BaseExecutionAdapter
from src.trading_system.execution.shadow import ShadowExecutionAdapter
from src.trading_system.execution.paper import PaperExecutionAdapter
from src.trading_system.execution.live import LiveExecutionAdapter


def get_execution_adapter(
    trading_mode: str = "paper",
    confirm_live: bool = False,
    fee_rate: Optional[float] = None,
    slippage_rate: Optional[float] = None,
) -> BaseExecutionAdapter:
    """
    Factory function to return appropriate ExecutionAdapter based on trading_mode:
    - 'paper': Paper Trading (모의투자) using testnet/simulated BTCUSDT execution
    - 'live': Live Trading (실전투자) using real Binance API with safety guards
    - 'shadow': Shadow/Replay simulation for offline backtest & analysis
    """
    mode = (trading_mode or "paper").lower()

    if mode == "paper":
        kwargs = {}
        if fee_rate is not None:
            kwargs["fee_rate"] = fee_rate
        if slippage_rate is not None:
            kwargs["slippage_rate"] = slippage_rate
        return PaperExecutionAdapter(**kwargs)

    elif mode == "live":
        kwargs = {"confirm_live": confirm_live}
        if fee_rate is not None:
            kwargs["fee_rate"] = fee_rate
        return LiveExecutionAdapter(**kwargs)

    elif mode == "shadow":
        kwargs = {}
        if fee_rate is not None:
            kwargs["fee_rate"] = fee_rate
        if slippage_rate is not None:
            kwargs["slippage_rate"] = slippage_rate
        return ShadowExecutionAdapter(**kwargs)

    else:
        raise ValueError(
            f"Unknown trading_mode: {trading_mode!r}. Choose from ['paper', 'live', 'shadow']."
        )
