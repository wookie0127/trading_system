import uuid
import os
from typing import Dict, Any, Optional
from loguru import logger
from src.trading_system.execution.base import BaseExecutionAdapter


class PaperExecutionAdapter(BaseExecutionAdapter):
    """
    Paper Trading (모의투자) Execution Adapter for BTCUSDT.
    Simulates Binance testnet paper trading execution with realistic fee structures
    (0.075% taker fee for VIP0/BNB) and 0.01% market slippage.
    """

    def __init__(
        self,
        fee_rate: float = 0.00075,
        slippage_rate: float = 0.0001,
        initial_balance: float = 10000.0,
    ):
        self.fee_rate = fee_rate
        self.slippage_rate = slippage_rate
        self.paper_balance = float(os.getenv("PAPER_INITIAL_BALANCE", initial_balance))
        self.api_key = os.getenv("BINANCE_TESTNET_API_KEY") or os.getenv(
            "PAPER_API_KEY"
        )
        self.api_secret = os.getenv("BINANCE_TESTNET_API_SECRET") or os.getenv(
            "PAPER_API_SECRET"
        )
        self.base_url = "https://testnet.binance.vision"

    def execute_order(
        self,
        decision_id: str,
        side: str,
        order_type: str,
        requested_price: float,
        requested_quantity: float,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Executes a paper order for BTCUSDT (Testnet/Paper Mode).
        """
        cid = client_order_id or f"paper-btcusdt-{uuid.uuid4().hex[:8]}"

        # Calculate slippage
        if side == "BUY":
            filled_price = requested_price * (1.0 + self.slippage_rate)
        else:
            filled_price = requested_price * (1.0 - self.slippage_rate)

        trade_value = filled_price * requested_quantity
        fee = trade_value * self.fee_rate
        slippage = abs(filled_price - requested_price) * requested_quantity

        logger.info(
            f"[PAPER TRADING 📝] Executed BTCUSDT {side} order: "
            f"qty={requested_quantity:.4f}, req_px={requested_price:.2f}, "
            f"fill_px={filled_price:.2f}, fee={fee:.4f} USDT, order_id={cid}"
        )

        return {
            "order_id": cid,
            "decision_id": decision_id,
            "mode": "paper",
            "symbol": "BTCUSDT",
            "side": side,
            "order_type": order_type,
            "requested_price": requested_price,
            "requested_quantity": requested_quantity,
            "filled_price": filled_price,
            "filled_quantity": requested_quantity,
            "fee": fee,
            "slippage": slippage,
            "status": "FILLED",
        }
