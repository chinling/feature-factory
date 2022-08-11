import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session():
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    yield spark
    # spark.stop()
