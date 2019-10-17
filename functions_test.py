import pytest
from pyspark.sql import SparkSession

from functions import *

schema = ["col_a", "col_b", "col_c"]


@pytest.fixture(scope="module")
def spark():
  spark = SparkSession.builder.master('local[2]').getOrCreate()
  yield spark
  spark.stop()


def test_derive_new_dataframe_1(spark):
  df0 = spark.createDataFrame([("aaa", "XXX", "XXX")], schema)
  df1 = derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 26
  assert row.col_d == "AAA"
  assert row.col_e == -99
  assert row.col_f == 3


def test_derive_new_dataframe_2(spark):
  df0 = spark.createDataFrame([("XXXX", "XXX", "XXX")], schema)
  df1 = derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 26
  assert row.col_d == "BBB"
  assert row.col_e == -99
  assert row.col_f == 4


def test_derive_new_dataframe_3(spark):
  df0 = spark.createDataFrame([("XXXXXX", "XXX", "XXX")], schema)
  df1 = derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 26
  assert row.col_d == "BBB"
  assert row.col_e == -99
  assert row.col_f == 6


def test_derive_new_column_1(spark):
  df0 = spark.createDataFrame([("aaa",)], ["col_a"])
  df1 = df0.select(derive_new_column().alias("output"))
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 1
  assert row.output == "AAA"


def test_describe_column(spark):
  df0 = spark.createDataFrame([("aaa", "bbbb", "ccccccccccc", "dddddddddddddd")],
                              ["col_a", "col_b", "col_c", "col_d"])
  df1 = df0.select(
    describe_column("col_a").alias("describe_a"),
    describe_column("col_b").alias("describe_b"),
    describe_column("col_c").alias("describe_c"),
    describe_column("col_d").alias("describe_d"),
  )
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 4
  assert row.describe_a == "SHORT,ODD"
  assert row.describe_b == "SHORT,EVEN"
  assert row.describe_c == "LONG,ODD"
  assert row.describe_d == "LONG,EVEN"
