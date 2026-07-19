import uuid
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from loguru import logger
from prefect import flow, task

from src.trading_system.collectors.market import MarketDataCollector
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


@task(name="Build & Validate Snapshot", retries=1)
def build_and_validate_snapshot_task(
    df_4h: pd.DataFrame, current_idx: int
) -> Dict[str, Any]:
    builder = SnapshotBuilder()
    snapshot = builder.build_snapshot_from_df(df_4h, current_idx)

    is_valid = SnapshotValidator.validate(snapshot)
    return {"snapshot": snapshot, "is_valid": is_valid}


@task(name="Get Gemini Decision", retries=2, retry_delay_seconds=3)
def get_gemini_decision_task(
    client: GeminiDecisionClient, snapshot: Any, strategy_guidelines: str
) -> Dict[str, Any]:
    decision, raw_response, is_success = client.get_decision(
        snapshot, strategy_guidelines
    )
    return {
        "decision": decision,
        "raw_response": raw_response,
        "is_success": is_success,
    }


@task(name="Validate Policy & Risk")
def validate_policy_and_risk_task(
    decision: Any, snapshot: Any, kill_switch: KillSwitch
) -> Dict[str, Any]:
    if kill_switch.is_active:
        return {
            "passed": False,
            "reason": f"Kill Switch Active: {kill_switch.active_reason}",
        }

    passed, reason = PolicyValidator.validate_decision(decision, snapshot)
    return {"passed": passed, "reason": reason}


@task(name="Execute Shadow Order & Save Audit")
def execute_order_and_save_audit_task(
    db_repo: SQLiteRepository,
    execution_adapter: ShadowExecutionAdapter,
    decision_id: str,
    snapshot: Any,
    decision: Any,
    raw_response: str,
    validation_status: str,
    validation_reason: str,
    df_4h: pd.DataFrame,
    current_idx: int,
    account_balance: float = 10000.0,
) -> Dict[str, Any]:
    order_res = None
    action = decision.action
    entry_price = snapshot.price.close

    if validation_status == "VALID" and action in ["OPEN_LONG", "OPEN_SHORT"]:
        side = "LONG" if action == "OPEN_LONG" else "SHORT"
        order_side = "BUY" if action == "OPEN_LONG" else "SELL"

        stop_loss = StopLossCalculator.calculate_stop_loss(
            side=side,
            entry_price=entry_price,
            atr_14=snapshot.technical.atr_ratio * entry_price,
        )

        qty = PositionSizer.calculate_position_size(
            account_balance=account_balance,
            entry_price=entry_price,
            stop_loss_price=stop_loss,
        )

        if qty > 0:
            order_res = execution_adapter.execute_order(
                decision_id=decision_id,
                side=order_side,
                order_type="MARKET",
                requested_price=entry_price,
                requested_quantity=qty,
            )
            db_repo.save_order(order_res)

    run_record = {
        "id": decision_id,
        "run_at": snapshot.run_at,
        "symbol": snapshot.symbol,
        "model_name": "gemini-2.5-flash",
        "strategy_version": "v1.0",
        "snapshot_data": snapshot.model_dump(),
        "raw_response": raw_response,
        "parsed_action": action,
        "parsed_confidence": getattr(decision, "confidence", 0.0),
        "validation_status": validation_status,
        "validation_reason": validation_reason,
    }
    db_repo.save_decision_run(run_record)

    # Counterfactual outcomes
    cf_outcome = CounterfactualEvaluator.calculate_outcomes(df_4h, current_idx)
    cf_outcome["decision_id"] = decision_id
    db_repo.save_counterfactual_outcome(cf_outcome)

    return {
        "decision_id": decision_id,
        "action": action,
        "order": order_res,
        "counterfactual": cf_outcome,
    }


@flow(name="Gemini 4H Single-Shot Trading Flow")
def gemini_4h_trading_flow(
    db_path: str = "trading_system.db",
    strategy_path: str = "configs/strategies/btc_4h_adaptive.md",
    model_name: str = "gemini-2.5-flash",
) -> Dict[str, Any]:
    logger.info("Executing Prefect Flow: Gemini 4H Single-Shot Trading Flow")

    collector = MarketDataCollector()
    df_4h = collector.load_4h_candles()
    current_idx = len(df_4h) - 1

    # Load strategy guidelines
    strat_file = Path(strategy_path)
    guidelines = strat_file.read_text(encoding="utf-8") if strat_file.exists() else ""

    # Task 1: Build Snapshot
    snap_res = build_and_validate_snapshot_task(df_4h, current_idx)
    snapshot = snap_res["snapshot"]

    if not snap_res["is_valid"]:
        logger.warning("Snapshot is invalid/stale. Halting flow.")
        return {"status": "INVALID_SNAPSHOT"}

    # Task 2: Gemini Decision
    llm_client = GeminiDecisionClient(model_name=model_name)
    decision_res = get_gemini_decision_task(llm_client, snapshot, guidelines)

    decision_id = f"dec-{uuid.uuid4().hex[:10]}"
    decision = decision_res["decision"]
    raw_response = decision_res["raw_response"]

    if not decision_res["is_success"]:
        val_status, val_reason = "INVALID", f"Gemini API Error: {raw_response}"
    else:
        # Task 3: Policy & Risk Check
        kill_switch = KillSwitch()
        pol_res = validate_policy_and_risk_task(decision, snapshot, kill_switch)
        val_status = "VALID" if pol_res["passed"] else "INVALID"
        val_reason = "Pass" if pol_res["passed"] else pol_res["reason"]

    # Task 4: Order Execution & Storage
    db_repo = SQLiteRepository(db_path=db_path)
    execution_adapter = ShadowExecutionAdapter()

    res = execute_order_and_save_audit_task(
        db_repo=db_repo,
        execution_adapter=execution_adapter,
        decision_id=decision_id,
        snapshot=snapshot,
        decision=decision,
        raw_response=raw_response,
        validation_status=val_status,
        validation_reason=val_reason,
        df_4h=df_4h,
        current_idx=current_idx,
    )

    logger.info(
        f"Prefect Flow completed: Action={res['action']}, DecisionID={res['decision_id']}"
    )
    return res


@flow(name="Gemini Historical Replay Flow")
def gemini_replay_flow(
    max_cycles: Optional[int] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_idx: int = 50,
    db_path: str = "trading_system.db",
) -> Dict[str, Any]:
    """
    Prefect Flow for Replaying Historical Candles.
    Can filter by start_date / end_date (e.g. '2026-01-01') or max_cycles.
    If max_cycles is None and dates are None, replays all available candles in DB/file.
    """
    logger.info("Executing Prefect Flow: Gemini Historical Replay Flow")

    collector = MarketDataCollector()
    df_4h = collector.load_4h_candles()
    df_4h["timestamp_dt"] = pd.to_datetime(df_4h["timestamp"])

    # Date Range Filtering
    if start_date:
        df_filtered = df_4h[df_4h["timestamp_dt"] >= pd.to_datetime(start_date)]
        if not df_filtered.empty:
            start_idx = max(50, df_filtered.index[0])

    if end_date:
        df_filtered = df_4h[df_4h["timestamp_dt"] <= pd.to_datetime(end_date)]
        if not df_filtered.empty:
            max_end = df_filtered.index[-1]
        else:
            max_end = len(df_4h) - 7
    else:
        max_end = len(df_4h) - 7

    if max_cycles is not None and max_cycles > 0:
        end_idx = min(start_idx + max_cycles, max_end)
    else:
        end_idx = max_end

    logger.info(
        f"Replay range: candle index {start_idx} to {end_idx} (Total: {max(0, end_idx - start_idx)} candles)"
    )

    db_repo = SQLiteRepository(db_path=db_path)
    llm_client = GeminiDecisionClient(model_name="gemini-2.5-flash")
    execution_adapter = ShadowExecutionAdapter()
    kill_switch = KillSwitch()

    strat_file = Path("configs/strategies/btc_4h_adaptive.md")
    guidelines = strat_file.read_text(encoding="utf-8") if strat_file.exists() else ""

    results = []
    for i in range(start_idx, end_idx):
        snap_res = build_and_validate_snapshot_task(df_4h, i)
        snapshot = snap_res["snapshot"]

        decision_id = f"dec-{uuid.uuid4().hex[:10]}"
        decision_res = get_gemini_decision_task(llm_client, snapshot, guidelines)
        decision = decision_res["decision"]

        pol_res = validate_policy_and_risk_task(decision, snapshot, kill_switch)
        val_status = (
            "VALID" if (decision_res["is_success"] and pol_res["passed"]) else "INVALID"
        )
        val_reason = "Pass" if pol_res["passed"] else pol_res["reason"]

        res = execute_order_and_save_audit_task(
            db_repo=db_repo,
            execution_adapter=execution_adapter,
            decision_id=decision_id,
            snapshot=snapshot,
            decision=decision,
            raw_response=decision_res["raw_response"],
            validation_status=val_status,
            validation_reason=val_reason,
            df_4h=df_4h,
            current_idx=i,
        )
        results.append(res)

    logger.info(f"Prefect Replay Flow completed. Replayed {len(results)} cycles.")
    return {"total_cycles": len(results), "results": results}


if __name__ == "__main__":
    gemini_4h_trading_flow()
