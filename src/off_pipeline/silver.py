"""Silver: cleaned, typed and de-duplicated products (one row per barcode).

Steps:
1. flatten   - pick the fields we need, cast nutrients to numbers
2. normalise - clean tag values ("en:united-kingdom" -> "united kingdom")
3. dedupe    - keep only the newest version of each barcode
4. validate  - data quality rules, failed rows go to a quarantine table
5. MERGE     - upsert into the Silver Delta table by barcode
"""

import logging

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from off_pipeline.config import Paths
from off_pipeline.quality import split_valid_rejected

log = logging.getLogger(__name__)

SILVER_COLUMNS = [
    "code",
    "product_name",
    "brand",
    "main_category",
    "countries",
    "sugar_100g",
    "energy_100g_kj",
    "protein_100g",
    "last_modified_at",
    "_ingested_at",
]


def _clean_tag(col: Column) -> Column:
    """'en:united-kingdom' -> 'united kingdom'."""
    no_prefix = F.regexp_replace(col, r"^[a-z]{2,3}:", "")
    return F.trim(F.regexp_replace(F.lower(no_prefix), "-", " "))


def _nutrient(key: str) -> Column:
    return F.col("nutriments").getItem(key).cast("double")


def flatten(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.trim("code").alias("code"),
        F.col("product_name"),
        # The first brand tag is the main brand. Categories are ordered from
        # general to specific, so the first one is the broad main category.
        F.element_at("brands_tags", 1).alias("brand"),
        F.element_at("categories_tags", 1).alias("main_category"),
        F.coalesce("countries_tags", F.array().cast("array<string>")).alias("countries"),
        _nutrient("sugars_100g").alias("sugar_100g"),
        # Open Food Facts stores energy_100g in kJ
        _nutrient("energy_100g").alias("energy_100g_kj"),
        _nutrient("proteins_100g").alias("protein_100g"),
        F.timestamp_seconds("last_modified_t").alias("last_modified_at"),
        F.col("_ingested_at"),
    )


def normalise(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("brand", _clean_tag(F.col("brand")))
        .withColumn("main_category", _clean_tag(F.col("main_category")))
        .withColumn("countries", F.array_distinct(F.transform("countries", _clean_tag)))
        .withColumn("product_name", F.trim(F.regexp_replace("product_name", r"\s+", " ")))
        .withColumn("sugar_100g", F.round("sugar_100g", 2))
        .withColumn("energy_100g_kj", F.round("energy_100g_kj", 1))
        .withColumn("protein_100g", F.round("protein_100g", 2))
    )


def dedupe_latest(df: DataFrame) -> DataFrame:
    """Keep one row per barcode: the most recently modified, then most recently ingested."""
    window = Window.partitionBy("code").orderBy(
        F.col("last_modified_at").desc_nulls_last(),
        F.col("_ingested_at").desc(),
    )
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def transform(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Bronze rows -> (valid Silver rows, rejected rows)."""
    cleaned = dedupe_latest(normalise(flatten(bronze)))
    valid, rejected = split_valid_rejected(cleaned)
    return valid.select(*SILVER_COLUMNS), rejected


def merge_into_silver(spark: SparkSession, updates: DataFrame, path: str) -> None:
    """Upsert by barcode. Existing rows are only replaced by newer versions,
    so running the job twice gives the same result (idempotent)."""
    if not DeltaTable.isDeltaTable(spark, path):
        updates.write.format("delta").save(path)
        return

    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(updates.alias("s"), "t.code = s.code")
        .whenMatchedUpdateAll(condition="s.last_modified_at > t.last_modified_at")
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver(spark: SparkSession, paths: Paths) -> None:
    bronze = spark.read.format("delta").load(paths.bronze_products)
    valid, rejected = transform(bronze)

    merge_into_silver(spark, valid, paths.silver_products)
    # The quarantine table is a full snapshot of what currently fails the rules
    rejected.write.format("delta").mode("overwrite").save(paths.silver_rejected)

    n_silver = spark.read.format("delta").load(paths.silver_products).count()
    n_rejected = spark.read.format("delta").load(paths.silver_rejected).count()
    log.info("Silver products: %d rows | rejected: %d rows", n_silver, n_rejected)
