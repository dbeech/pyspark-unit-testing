import pytest

from pyspark.sql import SparkSession
import functions as F

schema = ["col_a", "col_b", "col_c"]

@pytest.fixture
def spark():
  return SparkSession.builder.master('local[2]').getOrCreate()

def test_case_1(spark):
  df0 = spark.createDataFrame([("aaa", "XXX", "XXX")], schema)
  df1 = F.derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 6
  assert row.col_d == "AAA"
  assert row.col_e == -99
  assert row.col_f == 3
  
def test_case_2(spark):
  df0 = spark.createDataFrame([("XXXX", "XXX", "XXX")], schema)
  df1 = F.derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 6
  assert row.col_d == "BBB"
  assert row.col_e == -99
  assert row.col_f == 4
  
def test_case_3(spark):
  df0 = spark.createDataFrame([("XXXXXX", "XXX", "XXX")], schema)
  df1 = F.derive_new_dataframe(df0)
  assert df1.count() == 1
  row = df1.collect()[0]
  assert len(row) == 6
  assert row.col_d == "BBB"
  assert row.col_e == -99
  assert row.col_f == 6
