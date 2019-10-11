from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Delete previous output
!hdfs dfs -rm -r -skipTrash output

# Create Spark session
spark = SparkSession\
  .builder\
  .appName("PySpark Testing")\
  .config("spark.driver.memory", "2g")\
  .config("spark.executor.cores", 4)\
  .config("spark.dynamicAllocation.enabled", "true")\
  .config("spark.dynamicAllocation.maxExecutors", 8)\
  .getOrCreate()

# Read input CSV from HDFS
df0 = spark.read.csv('input', header=True)

# Reset metrics for query planning/analysis time
spark.sparkContext._jvm.org.apache.spark.sql.catalyst.rules.RuleExecutor.resetMetrics()

# Derive new columns
df1 = df0.select('*',
  when(col("col_a") == "aaa", "AAA").otherwise("BBB").alias("col_d"),
  lit(-99).alias("col_e"),
  length("col_b").alias("col_f"),
  lit("frmbq").alias("col_g"),
  lit("qwjlc").alias("col_h"),
  lit("xmzgs").alias("col_i"),
  lit("htlof").alias("col_j"),
  lit("evass").alias("col_k"),
  lit("hvrmy").alias("col_l"),
  lit("svswp").alias("col_m"),
  lit("lfzdm").alias("col_n"),
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