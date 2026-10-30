"""Data quality rules for Silver.

Each rule is a name plus a condition that must be TRUE for a valid row.
Rows failing at least one rule are not dropped silently: they go to a
quarantine table together with the names of the rules they failed.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Max. energy per 100 g is ~3,700 kJ (pure fat). Anything above is a data entry error.
MAX_ENERGY_KJ = 3800
NON_FOOD_PATTERN = "cosmetic|cleaning|hygiene"


def _between(col: str, low: float, high: float) -> Column:
    return F.col(col).between(low, high)


def get_rules() -> dict[str, Column]:
    # Built inside a function: Column objects need an active Spark session,
    # so they cannot be created at import time.
    return {
        "code_present": F.col("code").isNotNull() & (F.trim("code") != ""),
        "product_name_present": F.col("product_name").isNotNull() & (F.col("product_name") != ""),
        "brand_present": F.col("brand").isNotNull(),
        "category_present": F.col("main_category").isNotNull(),
        "country_present": F.size("countries") > 0,
        "sugar_in_range": _between("sugar_100g", 0, 100),
        "protein_in_range": _between("protein_100g", 0, 100),
        "energy_in_range": _between("energy_100g_kj", 0, MAX_ENERGY_KJ),
        "is_food": ~F.col("main_category").rlike(NON_FOOD_PATTERN),
    }


def apply_rules(df: DataFrame, rules: dict[str, Column] | None = None) -> DataFrame:
    """Add `_dq_failed_rules`: an array with the names of all failed rules.

    A NULL condition (e.g. missing sugar value) counts as failed.
    """
    rules = rules or get_rules()
    failed = [
        F.when(F.coalesce(condition, F.lit(False)), F.lit(None)).otherwise(F.lit(name))
        for name, condition in rules.items()
    ]
    return df.withColumn("_dq_failed_rules", F.array_compact(F.array(*failed)))


def split_valid_rejected(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    checked = apply_rules(df)
    is_valid = F.size("_dq_failed_rules") == 0
    valid = checked.filter(is_valid).drop("_dq_failed_rules")
    rejected = checked.filter(~is_valid)
    return valid, rejected
