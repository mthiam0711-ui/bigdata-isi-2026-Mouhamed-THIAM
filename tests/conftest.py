import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .master("local[1]")
             .appName("tests-tp3")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
