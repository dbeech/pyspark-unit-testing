from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType

from functions import random_string

# Delete previous output
#!hdfs dfs -rm -r -skipTrash output

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("PySpark Testing") \
  .config("spark.driver.memory", "2g") \
  .config("spark.executor.cores", 4) \
  .config("spark.dynamicAllocation.enabled", "true") \
  .config("spark.dynamicAllocation.maxExecutors", 8) \
  .getOrCreate()

spark.sparkContext.addPyFile('functions.py')

# Read input CSV from HDFS
df0 = spark.read.csv('input', header=True)

# Reset metrics for query planning/analysis time
spark.sparkContext._jvm.org.apache.spark.sql.catalyst.rules.RuleExecutor.resetMetrics()

random_string_udf = udf(random_string, StringType())

# Derive new columns
df1 = df0.select('*',
  when(col("col_a") == "aaa", "AAA").otherwise("BBB").alias("col_d"),
  lit(-99).alias("col_e"),
  length("col_b").alias("col_f"),
  random_string_udf().alias("col_g"),
  random_string_udf().alias("col_h"),
  random_string_udf().alias("col_i"),
  random_string_udf().alias("col_j")
)\
.persist()\
.select('*',
  when(length("col_g") > 10, when(length("col_g") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
    .otherwise(when(length("col_g") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")).alias("col_k"),
  when(length("col_h") > 10, when(length("col_h") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
    .otherwise(when(length("col_h") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")).alias("col_l"),
  when(length("col_i") > 10, when(length("col_i") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
    .otherwise(when(length("col_i") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")).alias("col_m"),
  when(length("col_j") > 10, when(length("col_j") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
    .otherwise(when(length("col_j") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")).alias("col_n"),
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

# Dump metrics for query planning/analysis time
spark.sparkContext._jvm.org.apache.spark.sql.catalyst.rules.RuleExecutor.dumpTimeSpent()

# Output to HDFS
df1.write.csv('output')
