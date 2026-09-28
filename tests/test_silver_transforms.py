def test_spark_fixture_works(spark):
    df = spark.createDataFrame([(1, "a")], ["id", "val"])
    assert df.count() == 1
