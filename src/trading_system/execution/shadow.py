import uuid
from typing import Dict, Any, Optional
from src.trading_system.execution.base import BaseExecutionAdapter


class ShadowExecutionAdapter(BaseExecutionAdapter):
    def __init__(self, fee_rate: float = 0.0005, slippage_rate: float = 0.0002):
        self.fee_rate = fee_rate
        self.slippage_rate = slippage_rate

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
        Simulates execution without placing real exchange orders.
        Calculates realistic slippage and trading fee.
        """
        cid = client_order_id or f"shadow-{uuid.uuid4().hex[:8]}"

        # Apply slippage
        if side == "BUY":
            filled_price = requested_price * (1 + self.slippage_rate)
        else:
            filled_price = requested_price * (1 - self.slippage_rate)

        trade_value = filled_price * requested_quantity
        fee = trade_value * self.fee_rate
        slippage = abs(filled_price - requested_price) * requested_quantity

        return {
            "order_id": cid,
            "decision_id": decision_id,
            "mode": "shadow",
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
