import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, Union

try:
    from src.core.config import DB_PATH
except ImportError:
    from core.config import DB_PATH


class SQLiteRepository:
    def __init__(self, db_path: Union[str, Path] = DB_PATH):
        self.db_path = str(db_path)
        self._init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path)

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Decision Runs Table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS decision_runs (
                id TEXT PRIMARY KEY,
                run_at TEXT,
                symbol TEXT,
                model_name TEXT,
                strategy_version TEXT,
                snapshot_data TEXT,
                raw_response TEXT,
                parsed_action TEXT,
                parsed_confidence REAL,
                validation_status TEXT,
                validation_reason TEXT
            )
            """)

            # Orders Table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                decision_id TEXT,
                mode TEXT,
                side TEXT,
                order_type TEXT,
                requested_price REAL,
                requested_quantity REAL,
                filled_price REAL,
                filled_quantity REAL,
                fee REAL,
                slippage REAL,
                status TEXT,
                created_at TEXT
            )
            """)

            # Outcomes Table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS position_outcomes (
                decision_id TEXT PRIMARY KEY,
                return_4h REAL,
                return_24h REAL,
                maximum_favorable_excursion REAL,
                maximum_adverse_excursion REAL,
                hit_stop_loss INTEGER,
                realized_pnl REAL
            )
            """)

            # Counterfactual Outcomes Table
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS counterfactual_outcomes (
                decision_id TEXT PRIMARY KEY,
                hypothetical_long_return_4h REAL,
                hypothetical_short_return_4h REAL,
                hypothetical_long_return_24h REAL,
                hypothetical_short_return_24h REAL,
                best_action_4h TEXT
            )
            """)

            conn.commit()

    def save_decision_run(self, data: Dict[str, Any]):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            INSERT INTO decision_runs (
                id, run_at, symbol, model_name, strategy_version,
                snapshot_data, raw_response, parsed_action, parsed_confidence,
                validation_status, validation_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    data["id"],
                    data["run_at"],
                    data.get("symbol", "BTCUSDT"),
                    data.get("model_name", "gemini-2.5-flash"),
                    data.get("strategy_version", "v1.0"),
                    json.dumps(data.get("snapshot_data", {})),
                    data.get("raw_response", ""),
                    data.get("parsed_action", "NO_TRADE"),
                    data.get("parsed_confidence", 0.0),
                    data.get("validation_status", "VALID"),
                    data.get("validation_reason", ""),
                ),
            )
            conn.commit()

    def save_order(self, order_data: Dict[str, Any]):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            INSERT INTO orders (
                order_id, decision_id, mode, side, order_type,
                requested_price, requested_quantity, filled_price, filled_quantity,
                fee, slippage, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    order_data["order_id"],
                    order_data["decision_id"],
                    order_data["mode"],
                    order_data["side"],
                    order_data["order_type"],
                    order_data["requested_price"],
                    order_data["requested_quantity"],
                    order_data["filled_price"],
                    order_data["filled_quantity"],
                    order_data["fee"],
                    order_data["slippage"],
                    order_data["status"],
                    order_data.get("created_at", ""),
                ),
            )
            conn.commit()

    def save_counterfactual_outcome(self, outcome_data: Dict[str, Any]):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
            INSERT OR REPLACE INTO counterfactual_outcomes (
                decision_id, hypothetical_long_return_4h, hypothetical_short_return_4h,
                hypothetical_long_return_24h, hypothetical_short_return_24h, best_action_4h
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
                (
                    outcome_data["decision_id"],
                    outcome_data.get("hypothetical_long_return_4h", 0.0),
                    outcome_data.get("hypothetical_short_return_4h", 0.0),
                    outcome_data.get("hypothetical_long_return_24h", 0.0),
                    outcome_data.get("hypothetical_short_return_24h", 0.0),
                    outcome_data.get("best_action_4h", "NO_TRADE"),
                ),
            )
            conn.commit()
