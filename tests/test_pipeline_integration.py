"""End-to-end run on the fixture data with real Delta tables (no API calls)."""

import json
import shutil
from pathlib import Path

import pytest

from off_pipeline.bronze import run_bronze
from off_pipeline.config import Paths
from off_pipeline.gold import run_gold
from off_pipeline.silver import run_silver

FIXTURES = Path(__file__).parent / "fixtures"

pytestmark = pytest.mark.integration


def read(spark, path):
    return spark.read.format("delta").load(path)


def run_all(spark, paths):
    run_bronze(spark, paths)
    run_silver(spark, paths)
    run_gold(spark, paths)


def test_full_run_is_idempotent_and_merges_updates(spark, tmp_path):
    paths = Paths(base=str(tmp_path / "lakehouse"))
    landing = tmp_path / "lakehouse" / "landing" / "products" / "ingest_date=2026-10-01"
    landing.mkdir(parents=True)
    shutil.copy(FIXTURES / "products_page_1.jsonl", landing)

    # First run
    run_all(spark, paths)
    assert read(spark, paths.bronze_products).count() == 10
    assert read(spark, paths.silver_products).count() == 4
    assert read(spark, paths.silver_rejected).count() == 5
    assert read(spark, paths.gold("fact_product")).count() == 4

    # Second run without new files: nothing changes
    run_all(spark, paths)
    assert read(spark, paths.bronze_products).count() == 10
    assert read(spark, paths.silver_products).count() == 4

    # New landing file: a newer version of 1002 and a new product 1010
    updates = [
        {
            "code": "1002",
            "product_name": "Cola Zero",
            "brands_tags": ["coca-cola"],
            "categories_tags": ["en:beverages"],
            "countries_tags": ["en:united-kingdom"],
            "nutriments": {"sugars_100g": 0, "energy_100g": 1.3, "proteins_100g": 0},
            "last_modified_t": 1770000000,
        },
        {
            "code": "1010",
            "product_name": "Oat Flakes",
            "brands_tags": ["koelln"],
            "categories_tags": ["en:cereals"],
            "countries_tags": ["en:germany"],
            "nutriments": {"sugars_100g": 1.2, "energy_100g": 1560, "proteins_100g": 13.5},
            "last_modified_t": 1770000000,
        },
    ]
    with (landing / "run2_page_0001.jsonl").open("w") as f:
        f.writelines(json.dumps(p) + "\n" for p in updates)

    run_all(spark, paths)
    assert read(spark, paths.bronze_products).count() == 12  # only the new file was ingested

    silver = {r["code"]: r for r in read(spark, paths.silver_products).collect()}
    assert len(silver) == 5
    assert silver["1002"]["product_name"] == "Cola Zero"  # updated by MERGE
    assert "1010" in silver  # inserted by MERGE

    history = spark.sql(f"DESCRIBE HISTORY delta.`{paths.silver_products}`")
    assert "MERGE" in {r["operation"] for r in history.collect()}
