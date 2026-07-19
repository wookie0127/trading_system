import sys
import argparse
from pathlib import Path
from typing import Optional

# Ensure workspace root is in sys.path using pathlib
WORKSPACE_ROOT = Path(__file__).resolve().parent.parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from loguru import logger
from src.trading_system.orchestration.prefect_flow import gemini_4h_trading_flow, gemini_replay_flow


class GeminiTradingApp:
    def __init__(
        self,
        mode: str = "replay",
        model_name: str = "gemini-2.5-flash",
        db_path: str = "trading_system.db",
        strategy_path: str = "configs/strategies/btc_4h_adaptive.md"
    ):
        self.mode = mode
        self.db_path = db_path
        self.model_name = model_name
        self.strategy_path = strategy_path

    def run_replay(
        self,
        max_cycles: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        start_idx: int = 50
    ):
        """
        Runs historical replay via Prefect Flow.
        Can filter by start_date/end_date or max_cycles.
        If max_cycles is None and dates are None, tests all accumulated candles in DB/file.
        """
        logger.info("==================================================")
        logger.info(f" Starting Gemini Trading App via PREFECT FLOW - REPLAY MODE")
        if start_date or end_date:
            logger.info(f" Date Range: {start_date or 'BEGIN'} ~ {end_date or 'END'}")
        elif max_cycles:
            logger.info(f" Max Cycles: {max_cycles}")
        else:
            logger.info(" Replay Target: ALL accumulated candles in DB")
        logger.info("==================================================")

        res = gemini_replay_flow(
            max_cycles=max_cycles,
            start_date=start_date,
            end_date=end_date,
            start_idx=start_idx,
            db_path=self.db_path
        )

        logger.info("==================================================")
        logger.info(f" Prefect Replay Flow Completed! Executed {res['total_cycles']} cycles.")
        logger.info(f" Audit DB Path: {self.db_path}")
        logger.info("==================================================")

    def run_single_shot(self):
        """
        Runs single 4H decision cycle via Prefect Flow.
        """
        logger.info("==================================================")
        logger.info(" Starting Gemini Trading App via PREFECT FLOW - SINGLE-SHOT MODE")
        logger.info("==================================================")

        res = gemini_4h_trading_flow(
            db_path=self.db_path,
            strategy_path=self.strategy_path,
            model_name=self.model_name
        )

        logger.info(f"Prefect Flow Result: {res}")
        logger.info("==================================================")


def main():
    parser = argparse.ArgumentParser(description="Gemini Autonomous Trading System App (Prefect Orchestrated)")
    parser.add_argument("--mode", type=str, choices=["replay", "single"], default="replay", help="Execution mode: replay or single")
    parser.add_argument("--cycles", type=int, default=None, help="Number of 4h candles to replay (default: None for all or date range)")
    parser.add_argument("--start-date", type=str, default=None, help="Start date for replay (e.g. 2026-01-01)")
    parser.add_argument("--end-date", type=str, default=None, help="End date for replay (e.g. 2026-06-30)")
    parser.add_argument("--all", action="store_true", help="Replay all accumulated candles in DB")
    parser.add_argument("--db", type=str, default="trading_system.db", help="Audit database path")
    parser.add_argument("--model", type=str, default="gemini-2.5-flash", help="Gemini model name")

    args = parser.parse_args()

    app = GeminiTradingApp(
        mode=args.mode,
        model_name=args.model,
        db_path=args.db
    )

    if args.mode == "replay":
        app.run_replay(
            max_cycles=args.cycles,
            start_date=args.start_date,
            end_date=args.end_date
        )
    else:
        app.run_single_shot()


if __name__ == "__main__":
    main()
