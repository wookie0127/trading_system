import pandas as pd
from typing import Dict, Any


class CounterfactualEvaluator:
    @staticmethod
    def calculate_outcomes(df_4h: pd.DataFrame, current_idx: int) -> Dict[str, Any]:
        """
        Calculates 4h and 24h hypothetical returns for both LONG and SHORT actions.
        """
        current_close = float(df_4h.iloc[current_idx]['close'])
        
        # 4h return (1 candle ahead)
        close_4h = float(df_4h.iloc[current_idx + 1]['close']) if (current_idx + 1) < len(df_4h) else current_close
        long_ret_4h = (close_4h - current_close) / current_close
        short_ret_4h = (current_close - close_4h) / current_close

        # 24h return (6 candles ahead)
        close_24h = float(df_4h.iloc[current_idx + 6]['close']) if (current_idx + 6) < len(df_4h) else close_4h
        long_ret_24h = (close_24h - current_close) / current_close
        short_ret_24h = (current_close - close_24h) / current_close

        best_action_4h = "NO_TRADE"
        if long_ret_4h > 0.01 and long_ret_4h > short_ret_4h:
            best_action_4h = "OPEN_LONG"
        elif short_ret_4h > 0.01 and short_ret_4h > long_ret_4h:
            best_action_4h = "OPEN_SHORT"

        return {
            "hypothetical_long_return_4h": long_ret_4h,
            "hypothetical_short_return_4h": short_ret_4h,
            "hypothetical_long_return_24h": long_ret_24h,
            "hypothetical_short_return_24h": short_ret_24h,
            "best_action_4h": best_action_4h
        }
