import uuid
from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger

from src.trading_system.snapshots.builder import SnapshotBuilder
from src.trading_system.snapshots.validator import SnapshotValidator
from src.trading_system.llm.client import GeminiDecisionClient
from src.trading_system.policy.validator import PolicyValidator
from src.trading_system.risk.sizing import PositionSizer
from src.trading_system.risk.stop_loss import StopLossCalculator
from src.trading_system.risk.kill_switch import KillSwitch
from src.trading_system.execution.shadow import ShadowExecutionAdapter
from src.trading_system.storage.repository import SQLiteRepository
from src.trading_system.evaluation.outcomes import CounterfactualEvaluator


class DecisionJob:
    def __init__(
        self,
        db_repo: SQLiteRepository,
        llm_client: GeminiDecisionClient,
        execution_adapter: ShadowExecutionAdapter,
        kill_switch: KillSwitch
    ):
        self.db_repo = db_repo
        self.llm_client = llm_client
        self.execution_adapter = execution_adapter
        self.kill_switch = kill_switch
        self.snapshot_builder = SnapshotBuilder()

    def run_cycle(
        self,
        df_4h: pd.DataFrame,
        current_idx: int,
        account_balance: float = 10000.0,
        strategy_guidelines: str = ""
    ) -> Dict[str, Any]:
        decision_id = f"dec-{uuid.uuid4().hex[:10]}"

        # 1. Build Snapshot
        snapshot = self.snapshot_builder.build_snapshot_from_df(df_4h, current_idx)

        # 2. Validate Snapshot Completeness & Freshness (Fail-Closed)
        if not SnapshotValidator.validate(snapshot):
            logger.warning("Snapshot validation failed. Triggering Fail-closed NO_TRADE.")
            return self._record_and_return_fail(decision_id, snapshot, "Snapshot validation failed (Stale/Incomplete)", df_4h, current_idx)

        # 3. Check Kill Switch
        if self.kill_switch.is_active:
            logger.warning(f"Kill Switch Active ({self.kill_switch.active_reason}). Forcing NO_TRADE.")
            return self._record_and_return_fail(decision_id, snapshot, f"Kill Switch Active: {self.kill_switch.active_reason}", df_4h, current_idx)

        # 4. Call Gemini Decision Layer (with retry via client)
        decision, raw_response, is_success = self.llm_client.get_decision(snapshot, strategy_guidelines)
        if not is_success:
            return self._record_and_return_fail(decision_id, snapshot, f"Gemini API failure: {raw_response}", df_4h, current_idx)

        # 5. Policy Validator
        is_policy_valid, policy_msg = PolicyValidator.validate_decision(decision, snapshot)
        if not is_policy_valid:
            logger.warning(f"Policy validation failed: {policy_msg}")
            return self._record_and_return_fail(decision_id, snapshot, f"Policy Invalid: {policy_msg}", df_4h, current_idx)

        # 6. Risk Engine Calculation & Execution (Shadow/Paper)
        order_res = None
        action = decision.action
        entry_price = snapshot.price.close

        if action in ["OPEN_LONG", "OPEN_SHORT"]:
            side = "LONG" if action == "OPEN_LONG" else "SHORT"
            order_side = "BUY" if action == "OPEN_LONG" else "SELL"

            stop_loss = StopLossCalculator.calculate_stop_loss(
                side=side,
                entry_price=entry_price,
                atr_14=snapshot.technical.atr_ratio * entry_price
            )

            qty = PositionSizer.calculate_position_size(
                account_balance=account_balance,
                entry_price=entry_price,
                stop_loss_price=stop_loss
            )

            if qty > 0:
                order_res = self.execution_adapter.execute_order(
                    decision_id=decision_id,
                    side=order_side,
                    order_type="MARKET",
                    requested_price=entry_price,
                    requested_quantity=qty
                )
                self.db_repo.save_order(order_res)

        # 7. Audit DB Save
        run_record = {
            "id": decision_id,
            "run_at": snapshot.run_at,
            "symbol": snapshot.symbol,
            "model_name": self.llm_client.model_name,
            "strategy_version": "v1.0",
            "snapshot_data": snapshot.model_dump(),
            "raw_response": raw_response,
            "parsed_action": decision.action,
            "parsed_confidence": decision.confidence,
            "validation_status": "VALID",
            "validation_reason": "Pass"
        }
        self.db_repo.save_decision_run(run_record)

        # 8. Counterfactual Evaluation
        cf_outcome = CounterfactualEvaluator.calculate_outcomes(df_4h, current_idx)
        cf_outcome["decision_id"] = decision_id
        self.db_repo.save_counterfactual_outcome(cf_outcome)

        return {
            "decision_id": decision_id,
            "action": decision.action,
            "confidence": decision.confidence,
            "order": order_res,
            "counterfactual": cf_outcome
        }

    def _record_and_return_fail(
        self,
        decision_id: str,
        snapshot: Any,
        reason: str,
        df_4h: Optional[pd.DataFrame] = None,
        current_idx: Optional[int] = None
    ) -> Dict[str, Any]:
        fail_record = {
            "id": decision_id,
            "run_at": snapshot.run_at if hasattr(snapshot, "run_at") else "",
            "symbol": "BTCUSDT",
            "model_name": self.llm_client.model_name,
            "strategy_version": "v1.0",
            "snapshot_data": snapshot.model_dump() if hasattr(snapshot, "model_dump") else {},
            "raw_response": reason,
            "parsed_action": "NO_TRADE",
            "parsed_confidence": 0.0,
            "validation_status": "INVALID",
            "validation_reason": reason
        }
        self.db_repo.save_decision_run(fail_record)

        if df_4h is not None and current_idx is not None:
            cf_outcome = CounterfactualEvaluator.calculate_outcomes(df_4h, current_idx)
            cf_outcome["decision_id"] = decision_id
            self.db_repo.save_counterfactual_outcome(cf_outcome)

        return {"decision_id": decision_id, "action": "NO_TRADE", "reason": reason}
