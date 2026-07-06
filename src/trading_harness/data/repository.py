from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from trading_harness.config import project_path


class ParquetRepository:
    def __init__(self, base_dir: str | Path = ".") -> None:
        self.base_dir = project_path(base_dir)

    def resolve(self, path: str | Path) -> Path:
        path = Path(path)
        if path.is_absolute():
            return path
        return self.base_dir / path

    def exists(self, path: str | Path) -> bool:
        return self.resolve(path).exists()

    def read(self, path: str | Path) -> pl.DataFrame:
        resolved = self.resolve(path)
        escaped = str(resolved).replace("'", "''")
        if resolved.suffix == ".parquet":
            return duckdb.sql(f"SELECT * FROM read_parquet('{escaped}')").pl()
        if resolved.suffix == ".csv":
            return duckdb.sql(f"SELECT * FROM read_csv_auto('{escaped}')").pl()
        raise ValueError(f"Unsupported data format: {resolved}")

    def write(self, df: pl.DataFrame, path: str | Path) -> Path:
        output_path = self.resolve(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.write_parquet(output_path)
        return output_path
