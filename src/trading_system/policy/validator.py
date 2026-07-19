from typing import Tuple
from src.trading_system.llm.schemas import TradingDecision
from src.trading_system.snapshots.schemas import MarketSnapshot
from src.trading_system.policy.state_machine import PositionStateMachine


class PolicyValidator:
    @staticmethod
    def validate_decision(
        decision: TradingDecision, snapshot: MarketSnapshot
    ) -> Tuple[bool, str]:
        """
        Validates if Gemini decision adheres to position state machine and policies.
        Returns: (is_valid, reason_message)
        """
        current_state = snapshot.position.status
        action = decision.action

        if not PositionStateMachine.is_valid_transition(current_state, action):
            return (
                False,
                f"Action {action} is invalid for current position state {current_state}",
            )

        # Prohibit OPEN when confidence is zero or negative
        if action in ["OPEN_LONG", "OPEN_SHORT"] and decision.confidence <= 0.0:
            return (
                False,
                f"Cannot open position with zero or negative confidence: {decision.confidence}",
            )

        return True, "Valid"
