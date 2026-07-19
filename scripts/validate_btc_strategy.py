import os
import sys
import polars as pl
from pathlib import Path

# Add src/ to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from loguru import logger
from backtest.engine import run_backtest
from strategies.moving_average_opt import optimize_ma_crossover

def run_validation():
    data_path = "data/processed/BTCUSDT_1h.parquet"
    if not os.path.exists(data_path):
        logger.error(f"Processed dataset not found at {data_path}. Run binance_collector and resampler first.")
        return

    # 1. Optimize parameters
    logger.info("Running optimization on 1-hour candles...")
    best_params = optimize_ma_crossover(data_path, n_trials=100)
    
    # 2. Load dataset
    df = pl.read_parquet(data_path)

    # 3. Run backtest with best parameters
    logger.info("Running backtest with optimized parameters...")
    opt_result = run_backtest(
        df,
        symbol="BTCUSDT",
        fast_window=best_params["fast_window"],
        slow_window=best_params["slow_window"],
        stop_loss_pct=best_params["stop_loss_pct"],
        take_profit_pct=best_params["take_profit_pct"]
    )
    opt_metrics = opt_result.metrics

    # 4. Run backtest with default parameters (MA 20/40, no SL/TP)
    logger.info("Running backtest with baseline parameters (MA 20/40, No SL/TP)...")
    base_result = run_backtest(
        df,
        symbol="BTCUSDT",
        fast_window=20,
        slow_window=40,
        stop_loss_pct=None,
        take_profit_pct=None
    )
    base_metrics = base_result.metrics

    # 5. Print Comparison Table
    print("\n" + "="*60)
    print(f" BACKTEST VALIDATION REPORT: BTCUSDT (1-Hour timeframe)")
    print("="*60)
    print(f"{'Metric':<25} | {'Baseline (MA20/40)':<20} | {'Optimized':<20}")
    print("-"*60)
    
    metrics_to_show = [
        ("Total Return", "total_return", "{:.2%}"),
        ("CAGR", "cagr", "{:.2%}"),
        ("Max Drawdown (MDD)", "mdd", "{:.2%}"),
        ("Sharpe Ratio", "sharpe", "{:.4f}"),
        ("Profit Factor", "profit_factor", "{:.4f}"),
        ("Win Rate", "win_rate", "{:.2%}"),
        ("Number of Trades", "num_trades", "{:d}"),
        ("Avg Holding Time", "average_holding_time", "{}"),
        ("Benchmark (Buy&Hold)", "benchmark_return", "{:.2%}"),
    ]
    
    for label, key, fmt in metrics_to_show:
        base_val = base_metrics.get(key, 0)
        opt_val = opt_metrics.get(key, 0)
        
        base_str = fmt.format(base_val) if isinstance(base_val, (int, float)) else str(base_val)
        opt_str = fmt.format(opt_val) if isinstance(opt_val, (int, float)) else str(opt_val)
        
        print(f"{label:<25} | {base_str:<20} | {opt_str:<20}")
    
    print("="*60)
    print("Optimized Parameters:")
    for k, v in best_params.items():
        print(f" - {k}: {v}")
    print("="*60 + "\n")

if __name__ == "__main__":
    run_validation()
