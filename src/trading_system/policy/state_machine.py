from typing import Literal

PositionState = Literal["FLAT", "LONG", "SHORT"]
ActionType = Literal["OPEN_LONG", "OPEN_SHORT", "HOLD", "REDUCE", "CLOSE", "NO_TRADE"]


class PositionStateMachine:
    @staticmethod
    def is_valid_transition(current_state: PositionState, action: ActionType) -> bool:
        """
        Validates state machine transitions as defined in design spec Section 11.
        FLAT: OPEN_LONG -> LONG, OPEN_SHORT -> SHORT, NO_TRADE -> FLAT
        LONG: HOLD -> LONG, REDUCE -> LONG, CLOSE -> FLAT
        SHORT: HOLD -> SHORT, REDUCE -> SHORT, CLOSE -> FLAT
        Immediate reversal (e.g. LONG -> OPEN_SHORT) is INVALID within same cycle.
        """
        if current_state == "FLAT":
            return action in ["OPEN_LONG", "OPEN_SHORT", "NO_TRADE"]
        elif current_state == "LONG":
            return action in ["HOLD", "REDUCE", "CLOSE"]
        elif current_state == "SHORT":
            return action in ["HOLD", "REDUCE", "SHORT", "REDUCE", "CLOSE"]
        return False

    @staticmethod
    def get_next_state(current_state: PositionState, action: ActionType) -> PositionState:
        if not PositionStateMachine.is_valid_transition(current_state, action):
            raise ValueError(f"Invalid transition from {current_state} with action {action}")
        
        if current_state == "FLAT":
            if action == "OPEN_LONG":
                return "LONG"
            elif action == "OPEN_SHORT":
                return "SHORT"
            return "FLAT"
        elif current_state in ["LONG", "SHORT"]:
            if action == "CLOSE":
                return "FLAT"
            return current_state
        return current_state
