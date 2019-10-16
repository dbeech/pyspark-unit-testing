import pytest

from pyspark.sql import SparkSession
from functions import derive_new_dataframe, derive_new_column

schema = ["col_a", "col_b", "col_c"]


@pytest.fixture
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
