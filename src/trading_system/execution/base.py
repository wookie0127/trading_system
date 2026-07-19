from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class BaseExecutionAdapter(ABC):
    @abstractmethod
    def execute_order(
        self,
        decision_id: str,
        side: str,  # "BUY" or "SELL"
        order_type: str,  # "MARKET" or "LIMIT"
        requested_price: float,
        requested_quantity: float,
        client_order_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        pass
