import optuna
import polars as pl
from pathlib import Path
from loguru import logger
from backtest.engine import run_backtest

# Suppress Optuna output logs unless warning
optuna.logging.set_verbosity(optuna.logging.WARNING)


def optimize_ma_crossover(
    data_path: str | Path,
    n_trials: int = 50,
    initial_balance: float = 1_000_000.0,
    fee: float = 0.0005,
    slippage: float = 0.0005,
) -> dict:
    df = pl.read_parquet(data_path)
    symbol = df["symbol"][0] if df.height > 0 else "BTCUSDT"

    def objective(trial: optuna.Trial) -> float:
        fast_window = trial.suggest_int("fast_window", 5, 50)
        slow_window = trial.suggest_int("slow_window", fast_window + 2, 150)
        stop_loss_pct = trial.suggest_float("stop_loss_pct", 0.01, 0.05, step=0.005)
        take_profit_pct = trial.suggest_float("take_profit_pct", 0.02, 0.15, step=0.005)

        try:
            result = run_backtest(
                df,
                symbol=symbol,
                fast_window=fast_window,
                slow_window=slow_window,
                initial_balance=initial_balance,
                fee=fee,
                slippage=slippage,
                stop_loss_pct=stop_loss_pct,
                take_profit_pct=take_profit_pct,
            )
            metrics = result.metrics
            # Optimize for Sharpe Ratio
            sharpe = metrics.get("sharpe", 0.0)
            # Penalize strategies with less than 2 trades to avoid noise/luck
            num_trades = metrics.get("num_trades", 0)
            if num_trades < 3:
                return -10.0 + num_trades
            return sharpe
        except Exception as e:
            logger.error(f"Trial failed: {e}")
            return -999.0

    study = optuna.create_study(direction="maximize")
    logger.info(
        f"Starting Optuna optimization for {symbol} on {Path(data_path).name} with {n_trials} trials..."
    )
    study.optimize(objective, n_trials=n_trials)

    logger.success(f"Optimization completed. Best trial Sharpe: {study.best_value:.4f}")
    logger.info(f"Best parameters: {study.best_params}")
    return study.best_params


if __name__ == "__main__":
    optimize_ma_crossover("data/processed/BTCUSDT_1h.parquet", n_trials=50)
