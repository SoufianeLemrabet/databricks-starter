# tests/conftest.py
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("tests")
        .config(
            "spark.sql.shuffle.partitions", "1"
        )  # évite 200 partitions pour de minuscules données
        .getOrCreate()
    )
    yield session
    session.stop()
