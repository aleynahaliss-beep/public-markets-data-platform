"""Execute the analyst-facing Spark SQL reports in /sql against the pipeline's output views."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession

from .io_utils import write_df_csv


def run_sql_reports(spark: SparkSession, sql_dir: str | Path, out_dir: str | Path, show: int = 0) -> dict[str, int]:
    counts: dict[str, int] = {}
    for path in sorted(Path(sql_dir).glob("*.sql")):
        df = spark.sql(path.read_text(encoding="utf-8"))
        write_df_csv(df, Path(out_dir) / f"{path.stem}.csv")
        counts[path.stem] = df.count()
        if show:
            print(f"\n-- {path.name}")
            df.show(show, truncate=False)
    return counts
