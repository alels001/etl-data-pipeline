"""Command line entry point.

    python -m off_pipeline                          # all steps, 10 API pages
    python -m off_pipeline --steps silver gold      # rebuild from Bronze, no API calls
    python -m off_pipeline --max-pages 5
"""

import argparse
import logging

from off_pipeline.bronze import run_bronze
from off_pipeline.config import get_paths
from off_pipeline.extract import run_extract
from off_pipeline.gold import run_gold
from off_pipeline.silver import run_silver
from off_pipeline.spark import get_spark

STEPS = ["extract", "bronze", "silver", "gold"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Open Food Facts lakehouse pipeline")
    parser.add_argument("--steps", nargs="+", choices=STEPS, default=STEPS)
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="API pages to download (100 products each, ~6.5 s per page)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    paths = get_paths()
    logging.info("Lakehouse path: %s", paths.base)

    if "extract" in args.steps:
        run_extract(paths.landing, max_pages=args.max_pages)

    spark_steps = [s for s in args.steps if s != "extract"]
    if not spark_steps:
        return

    spark = get_spark()
    try:
        # Keep the fixed order bronze -> silver -> gold, whatever order was passed
        for step, run in (("bronze", run_bronze), ("silver", run_silver), ("gold", run_gold)):
            if step in spark_steps:
                run(spark, paths)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
