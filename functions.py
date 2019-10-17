import random
import string

from pyspark.sql.functions import *
from pyspark.sql.types import StringType


def random_string(length=10):
  letters = string.ascii_lowercase
  return ''.join(random.choice(letters) for i in range(length))


def derive_new_dataframe(df0):
  random_string_udf = udf(random_string, StringType())
  return df0.select(
      '*',
      derive_new_column().alias("col_d"),
      lit(-99).alias("col_e"),
      length("col_a").alias("col_f"),
      random_string_udf().alias("col_g"),
      random_string_udf().alias("col_h"),
      random_string_udf().alias("col_i"),
      random_string_udf().alias("col_j")
    ) \
    .persist() \
    .select(
      '*',
      describe_column("col_g").alias("col_k"),
      describe_column("col_h").alias("col_l"),
      describe_column("col_i").alias("col_m"),
      describe_column("col_j").alias("col_n"),
      lit("nkmpn").alias("col_o"),
      lit("canun").alias("col_p"),
      lit("ibrfa").alias("col_q"),
      lit("icxyf").alias("col_r"),
      lit("tonhp").alias("col_s"),
      lit("ikggy").alias("col_t"),
      lit("wedtt").alias("col_u"),
      lit("phhhs").alias("col_v"),
      lit("tpvzm").alias("col_w"),
      lit("tehyp").alias("col_x"),
      lit("lnaae").alias("col_y"),
      lit("jdwfn").alias("col_z")
    )


def derive_new_column():
  return when(col('col_a') == 'aaa', 'AAA').otherwise('BBB')


def describe_column(col_name):
  return when(length(col_name) > 10, when(length(col_name) % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD")) \
    .otherwise(when(length(col_name) % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD"))
