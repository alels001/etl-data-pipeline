from pathlib import Path

import pytest
from pyspark.sql import functions as F

from off_pipeline.schemas import RAW_PRODUCT_SCHEMA
from off_pipeline.spark import get_spark

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark():
    session = get_spark("off-pipeline-tests")
    yield session
    session.stop()


@pytest.fixture
def bronze_df(spark):
    """The fixture page as it would look in Bronze."""
    return (
        spark.read.schema(RAW_PRODUCT_SCHEMA)
        .json(str(FIXTURES / "products_page_1.jsonl"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
