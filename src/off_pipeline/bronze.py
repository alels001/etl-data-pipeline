"""Bronze: load raw landing files into a Delta table, unchanged.

Bronze is our replayable copy of the source. We only add ingestion metadata,
so that Silver can always be rebuilt from here without calling the API again.

Incremental loading uses Spark Structured Streaming with `availableNow`:
the checkpoint remembers which files were already ingested, so each run picks
up only new files and then stops (a batch job with exactly-once file tracking).
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from off_pipeline.config import Paths
from off_pipeline.schemas import RAW_PRODUCT_SCHEMA

log = logging.getLogger(__name__)


def add_ingestion_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_ingest_date", F.to_date("_ingested_at"))
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


def run_bronze(spark: SparkSession, paths: Paths) -> None:
    raw = (
        spark.readStream.schema(RAW_PRODUCT_SCHEMA)
        .option("recursiveFileLookup", "true")
        .json(paths.landing)
    )

    query = (
        add_ingestion_metadata(raw)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", paths.bronze_checkpoint)
        .partitionBy("_ingest_date")
        .trigger(availableNow=True)
        .start(paths.bronze_products)
    )
    query.awaitTermination()

    total = spark.read.format("delta").load(paths.bronze_products).count()
    log.info("Bronze products: %d rows in total", total)
