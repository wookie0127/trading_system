import sys
import argparse
from pathlib import Path

# Add workspace root to pythonpath via pathlib
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from loguru import logger

from src.trading_system.collectors.market import MarketDataCollector
from src.trading_system.storage.repository import SQLiteRepository
from src.trading_system.llm.client import GeminiDecisionClient
from src.trading_system.execution.shadow import ShadowExecutionAdapter
from src.trading_system.risk.kill_switch import KillSwitch
from src.trading_system.orchestration.decision_job import DecisionJob


def run_historical_replay(
    max_cycles: int = 20, db_output_path: str = "trading_system.db"
):
    logger.info("Initializing Historical Replay Engine...")

    # Load 4H candles dataset
    collector = MarketDataCollector()
    df_4h = collector.load_4h_candles()
    logger.info(f"Loaded {len(df_4h)} candles for historical replay.")

    if len(df_4h) < 50:
        logger.error("Not enough candle data to perform replay (min 50 required).")
        return

    # Initialize components
    db_repo = SQLiteRepository(db_path=db_output_path)
    llm_client = GeminiDecisionClient(model_name="gemini-2.5-flash")
    execution_adapter = ShadowExecutionAdapter()
    kill_switch = KillSwitch()

    decision_job = DecisionJob(
        db_repo=db_repo,
        llm_client=llm_client,
        execution_adapter=execution_adapter,
        kill_switch=kill_switch,
    )

    # Strategy Guidelines
    strategy_path = Path("configs/strategies/btc_4h_adaptive.md")
    strategy_guidelines = ""
    if strategy_path.exists():
        strategy_guidelines = strategy_path.read_text(encoding="utf-8")

    start_idx = 50
    end_idx = min(start_idx + max_cycles, len(df_4h) - 7)

    logger.info(
        f"Starting Historical Replay loop from candle index {start_idx} to {end_idx}..."
    )

    success_count = 0
    no_trade_count = 0

    for i in range(start_idx, end_idx):
        candle_ts = df_4h.iloc[i]["timestamp"]
        logger.info(
            f"[{i - start_idx + 1}/{end_idx - start_idx}] Replaying candle at {candle_ts}..."
        )

        res = decision_job.run_cycle(
            df_4h=df_4h,
            current_idx=i,
            account_balance=10000.0,
            strategy_guidelines=strategy_guidelines,
        )

        action = res.get("action", "NO_TRADE")
        if action == "NO_TRADE":
            no_trade_count += 1
        else:
            success_count += 1
            logger.info(
                f"Executed action: {action} (Decision ID: {res.get('decision_id')})"
            )

    logger.info("=" * 50)
    logger.info("Historical Replay Completed Successfully!")
    logger.info(f"Total Cycles: {end_idx - start_idx}")
    logger.info(f"Active Trades: {success_count}, NO_TRADE: {no_trade_count}")
    logger.info(f"Results stored in audit DB: {db_output_path}")
    logger.info("=" * 50)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run Historical Replay for Gemini Trading System"
    )
    parser.add_argument(
        "--cycles", type=int, default=10, help="Number of 4h candles to replay"
    )
    parser.add_argument(
        "--db", type=str, default="trading_system.db", help="Output database file path"
    )
    args = parser.parse_args()

    run_historical_replay(max_cycles=args.cycles, db_output_path=args.db)
