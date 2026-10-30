"""Explicit schema for the raw API records.

Defining the schema instead of letting Spark infer it means:
- no extra pass over the files just to guess types,
- a changed API response cannot silently change our column types.

`nutriments` has hundreds of possible keys with mixed types, so Bronze keeps it
as a map of strings. Silver picks the keys it needs and casts them to numbers.
"""

from pyspark.sql.types import (
    ArrayType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)

RAW_PRODUCT_SCHEMA = StructType(
    [
        StructField("code", StringType()),
        StructField("product_name", StringType()),
        StructField("brands_tags", ArrayType(StringType())),
        StructField("categories_tags", ArrayType(StringType())),
        StructField("countries_tags", ArrayType(StringType())),
        StructField("nutriments", MapType(StringType(), StringType())),
        StructField("last_modified_t", LongType()),
    ]
)
