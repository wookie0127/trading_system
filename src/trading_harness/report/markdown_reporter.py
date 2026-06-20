from __future__ import annotations

from pathlib import Path

import polars as pl


class MarkdownReporter:
    def render(
        self,
        features: pl.DataFrame,
        signals: dict[str, pl.DataFrame],
        backtests: pl.DataFrame,
    ) -> str:
        latest_date = self._latest_date(features)
        signal_lines = self._render_signal_summary(signals)
        backtest_table = self._render_backtest_table(backtests)
        recent_summary = self._render_recent_summary(features)

        return "\n".join(
            [
                "# Daily Trading Research Report",
                "",
                "## 1. Market Summary",
                f"- Latest data date: {latest_date}",
                recent_summary,
                "",
                "## 2. Strategy Signals",
                signal_lines,
                "",
                "## 3. Backtest Results",
                backtest_table,
                "",
                "## 4. Risk Notes",
                "- This MVP is research-only and does not place live orders.",
                "- Backtests use close-to-close returns with fixed fee and slippage assumptions.",
                "- Korea inverse execution is represented as a signal until matching local market data is added.",
                "",
                "## 5. Next Actions",
                "- Review strategies with negative drawdown and low trade count before adding capital assumptions.",
                "- Add Korea inverse ETF price history for executable shock-signal backtests.",
                "- Add Slack or Telegram delivery behind an explicit notification command.",
                "",
            ]
        )

    def write(self, markdown: str, output_path: str | Path) -> Path:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(markdown, encoding="utf-8")
        return path

    def _latest_date(self, features: pl.DataFrame) -> str:
        if features.is_empty() or "date" not in features.columns:
            return "n/a"
        value = features.select(pl.col("date").max()).item()
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    def _render_signal_summary(self, signals: dict[str, pl.DataFrame]) -> str:
        rows = []
        for name, df in sorted(signals.items()):
            if df.is_empty():
                rows.append(f"- {name}: no signals generated")
                continue
            latest = df.sort("date").group_by("symbol").tail(1)
            active = latest.filter(pl.col("signal") == 1).get_column("symbol").to_list()
            rows.append(f"- {name}: active={', '.join(active) if active else 'none'}")
        return "\n".join(rows)

    def _render_backtest_table(self, backtests: pl.DataFrame) -> str:
        if backtests.is_empty():
            return "No backtest results available."
        cols = [
            "strategy",
            "symbol",
            "total_return",
            "annualized_return",
            "sharpe_ratio",
            "max_drawdown",
            "win_rate",
            "number_of_trades",
            "average_trade_return",
        ]
        rows = []
        for row in backtests.select(cols).iter_rows(named=True):
            rows.append(
                {
                    key: f"{value:.4f}" if isinstance(value, float) else value
                    for key, value in row.items()
                }
            )
        return self._to_markdown_table(cols, rows)

    def _render_recent_summary(self, features: pl.DataFrame) -> str:
        if features.is_empty():
            return "- No feature data available."
        rows = []
        for window in [30, 90]:
            subset = features.sort("date").group_by("symbol").tail(window)
            perf = (
                subset.group_by("symbol")
                .agg(((pl.col("return_1d").fill_null(0) + 1).product() - 1).alias("return"))
                .sort("return", descending=True)
                .head(3)
            )
            formatted = ", ".join(
                f"{row['symbol']}: {row['return']:.2%}" for row in perf.iter_rows(named=True)
            )
            rows.append(f"- Recent {window} rows return leaders: {formatted}")
        return "\n".join(rows)

    def _to_markdown_table(self, headers: list[str], rows: list[dict[str, object]]) -> str:
        lines = [
            "| " + " | ".join(headers) + " |",
            "| " + " | ".join(["---"] * len(headers)) + " |",
        ]
        for row in rows:
            values = [str(row[col]) for col in headers]
            lines.append("| " + " | ".join(values) + " |")
        return "\n".join(lines)
