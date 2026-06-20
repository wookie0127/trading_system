#!/usr/bin/env zsh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MODE="${1:-test}"

case "$MODE" in
  test)
    RUN_COMMAND="PYTHONPATH=src uv run pytest tests/test_features.py tests/test_strategy.py tests/test_backtest_metrics.py tests/test_agent_pipeline.py"
    WORKSPACE_NAME="Trading Harness Tests"
    ;;
  pipeline)
    RUN_COMMAND="PYTHONPATH=src uv run python -m trading_harness.supervisor"
    WORKSPACE_NAME="Trading Harness Pipeline"
    ;;
  report)
    RUN_COMMAND="PYTHONPATH=src uv run python scripts/run_report.py"
    WORKSPACE_NAME="Trading Harness Report"
    ;;
  agents)
    WORKSPACE_NAME="Trading Harness Agents"
    ;;
  *)
    echo "Usage: $0 [test|pipeline|report|agents]" >&2
    exit 2
    ;;
esac

if [[ "$MODE" == "agents" ]]; then
  LAYOUT=$(cat <<EOF
{
  "direction": "horizontal",
  "split": 0.34,
  "children": [
    {
      "direction": "vertical",
      "split": 0.5,
      "children": [
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[MarketDataAgent]' && PYTHONPATH=src uv run python scripts/run_market_data.py"
              }
            ]
          }
        },
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[FeatureAgent] waiting for raw parquet...' && until find data/raw -maxdepth 1 -name '*.parquet' 2>/dev/null | grep -q .; do sleep 10; done && PYTHONPATH=src uv run python scripts/run_features.py"
              }
            ]
          }
        }
      ]
    },
    {
      "direction": "vertical",
      "split": 0.5,
      "children": [
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[StrategyAgent] waiting for feature dataset...' && until test -f data/features/feature_dataset.parquet; do sleep 10; done && PYTHONPATH=src uv run python scripts/run_strategies.py"
              }
            ]
          }
        },
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[BacktestAgent] waiting for signals...' && until test -f data/signals/us_market_shock_inverse_signal.parquet && test -f data/signals/trend_following_signal.parquet; do sleep 10; done && PYTHONPATH=src uv run python scripts/run_backtest.py"
              }
            ]
          }
        }
      ]
    },
    {
      "direction": "vertical",
      "split": 0.5,
      "children": [
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[ReportAgent] waiting for backtest result...' && until test -f data/backtests/backtest_result.parquet; do sleep 10; done && PYTHONPATH=src uv run python scripts/run_report.py"
              }
            ]
          }
        },
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo '[Monitor] artifacts refresh every 15s' && while true; do clear; date; echo; find data/raw data/features data/signals data/backtests reports -maxdepth 1 -type f 2>/dev/null | sort; echo; test -f reports/daily_trading_research_report.md && sed -n '1,80p' reports/daily_trading_research_report.md; sleep 15; done"
              }
            ]
          }
        }
      ]
    }
  ]
}
EOF
  )
else
  LAYOUT=$(cat <<EOF
{
  "direction": "horizontal",
  "split": 0.62,
  "children": [
    {
      "pane": {
        "surfaces": [
          {
            "type": "terminal",
            "command": "cd '$ROOT_DIR' && echo '[trading-harness] mode=$MODE' && $RUN_COMMAND"
          }
        ]
      }
    },
    {
      "direction": "vertical",
      "split": 0.5,
      "children": [
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && git status --short && echo && rg -n 'import pandas|pd\\\\.|read_parquet|to_parquet\\\\(' src/trading_harness tests scripts || true"
              }
            ]
          }
        },
        {
          "pane": {
            "surfaces": [
              {
                "type": "terminal",
                "command": "cd '$ROOT_DIR' && echo 'Artifacts:' && find data/raw data/features data/signals data/backtests reports -maxdepth 1 -type f 2>/dev/null | sort && echo && test -f reports/daily_trading_research_report.md && sed -n '1,120p' reports/daily_trading_research_report.md || true"
              }
            ]
          }
        }
      ]
    }
  ]
}
EOF
  )
fi

cmux workspace create \
  --name "$WORKSPACE_NAME" \
  --description "Trading Research Harness: $MODE" \
  --cwd "$ROOT_DIR" \
  --layout "$LAYOUT" \
  --focus true
