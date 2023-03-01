__author__ = "ajjunior"

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("app-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()
