"""Gold: analytics-ready tables built from Silver.

Star schema
    dim_brand, dim_category, dim_country
    fact_product              one row per product, with nutrition features
    bridge_product_country    many-to-many: a product is sold in many countries
Aggregates
    agg_category_nutrition    nutrition profile per category
    agg_country_summary       product count and healthy share per country

Gold is rebuilt from Silver on every run (overwrite). At this data size that is
simpler and safer than incremental updates, and Silver stays the source of truth.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from off_pipeline.config import Paths

log = logging.getLogger(__name__)

# (low_upper, high_lower) per 100 g. Values between the two bounds are "medium".
SUGAR_G = (10, 20)
PROTEIN_G = (5, 15)
# energy_100g is in kJ: 400 kJ ~ 96 kcal, 1600 kJ ~ 382 kcal
ENERGY_KJ = (400, 1600)


def level(col: str, bounds: tuple[float, float]) -> Column:
    """0 = low, 1 = medium, 2 = high."""
    low_upper, high_lower = bounds
    return (
        F.when(F.col(col) < low_upper, 0)
        .when(F.col(col) <= high_lower, 1)
        .otherwise(2)
    )


def surrogate_key(col: str) -> Column:
    """Deterministic key from the natural key: the same name always gets the same id,
    so keys stay stable when Gold is rebuilt."""
    return F.xxhash64(F.col(col))


def build_dimension(silver: DataFrame, source_col: str, name: str) -> DataFrame:
    return (
        silver.select(F.col(source_col).alias(f"{name}_name"))
        .distinct()
        .withColumn(f"{name}_key", surrogate_key(f"{name}_name"))
        .select(f"{name}_key", f"{name}_name")
    )


def build_dim_country(silver: DataFrame) -> DataFrame:
    exploded = silver.select(F.explode("countries").alias("country"))
    return build_dimension(exploded, "country", "country")


def build_fact_product(silver: DataFrame) -> DataFrame:
    return silver.select(
        F.col("code").alias("product_code"),
        "product_name",
        surrogate_key("brand").alias("brand_key"),
        surrogate_key("main_category").alias("category_key"),
        "sugar_100g",
        "energy_100g_kj",
        "protein_100g",
        level("sugar_100g", SUGAR_G).alias("sugar_level"),
        level("energy_100g_kj", ENERGY_KJ).alias("energy_level"),
        level("protein_100g", PROTEIN_G).alias("protein_level"),
        ((F.col("sugar_100g") < 10) & (F.col("protein_100g") > 5)).alias("is_healthy"),
        "last_modified_at",
    )


def build_bridge_product_country(silver: DataFrame) -> DataFrame:
    return silver.select(
        F.col("code").alias("product_code"),
        F.explode("countries").alias("country"),
    ).select("product_code", surrogate_key("country").alias("country_key"))


def build_agg_category_nutrition(fact: DataFrame, dim_category: DataFrame) -> DataFrame:
    return (
        fact.join(dim_category, "category_key")
        .groupBy("category_name")
        .agg(
            F.count("*").alias("n_products"),
            F.round(F.avg("sugar_100g"), 1).alias("avg_sugar_100g"),
            F.round(F.avg("energy_100g_kj"), 0).alias("avg_energy_100g_kj"),
            F.round(F.avg("protein_100g"), 1).alias("avg_protein_100g"),
            F.round(F.avg(F.col("is_healthy").cast("int")), 3).alias("share_healthy"),
        )
    )


def build_agg_country_summary(
    fact: DataFrame, bridge: DataFrame, dim_country: DataFrame
) -> DataFrame:
    return (
        bridge.join(fact, "product_code")
        .join(dim_country, "country_key")
        .groupBy("country_name")
        .agg(
            F.count("*").alias("n_products"),
            F.round(F.avg(F.col("is_healthy").cast("int")), 3).alias("share_healthy"),
        )
    )


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    dim_brand = build_dimension(silver, "brand", "brand")
    dim_category = build_dimension(silver, "main_category", "category")
    dim_country = build_dim_country(silver)
    fact = build_fact_product(silver)
    bridge = build_bridge_product_country(silver)
    return {
        "dim_brand": dim_brand,
        "dim_category": dim_category,
        "dim_country": dim_country,
        "fact_product": fact,
        "bridge_product_country": bridge,
        "agg_category_nutrition": build_agg_category_nutrition(fact, dim_category),
        "agg_country_summary": build_agg_country_summary(fact, bridge, dim_country),
    }


def run_gold(spark: SparkSession, paths: Paths) -> None:
    silver = spark.read.format("delta").load(paths.silver_products)
    for name, df in build_gold(silver).items():
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(paths.gold(name))
        )
        n_rows = spark.read.format("delta").load(paths.gold(name)).count()
        log.info("Gold %-24s %d rows", name, n_rows)
