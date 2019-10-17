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
df1 = df0.withColumn("col_d",
                     when(col("col_a") == "aaa", "AAA")
                     .otherwise("BBB"))
df2 = df1.withColumn("col_e", lit(-99))
df3 = df2.withColumn("col_f", length("col_a"))
df4 = df3.withColumn("col_g", random_string_udf())
df5 = df4.withColumn("col_h", random_string_udf())
df6 = df5.withColumn("col_i", random_string_udf())
df7 = df6.withColumn("col_j", random_string_udf())
df8 = df7.withColumn("col_k",
                     when(length("col_g") > 10, when(length("col_g") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
                     .otherwise(when(length("col_g") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")))
df9 = df8.withColumn("col_l",
                     when(length("col_h") > 10, when(length("col_h") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
                     .otherwise(when(length("col_h") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")))
df10 = df9.withColumn("col_m",
                      when(length("col_i") > 10, when(length("col_i") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
                      .otherwise(when(length("col_i") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")))
df11 = df10.withColumn("col_n",
                       when(length("col_j") > 10, when(length("col_j") % 2 == 0, "LONG,EVEN").otherwise("LONG,ODD"))
                       .otherwise(when(length("col_j") % 2 == 0, "SHORT,EVEN").otherwise("SHORT,ODD")))
df12 = df11.withColumn("col_o", lit("nkmpn"))
df13 = df12.withColumn("col_p", lit("canun"))
df14 = df13.withColumn("col_q", lit("ibrfa"))
df15 = df14.withColumn("col_r", lit("icxyf"))
df16 = df15.withColumn("col_s", lit("tonhp"))
df17 = df16.withColumn("col_t", lit("ikggy"))
df18 = df17.withColumn("col_u", lit("wedtt"))
df19 = df18.withColumn("col_v", lit("phhhs"))
df20 = df19.withColumn("col_w", lit("tpvzm"))
df21 = df20.withColumn("col_x", lit("tehyp"))
df22 = df21.withColumn("col_y", lit("lnaae"))
df23 = df22.withColumn("col_z", lit("jdwfn"))

# Dump metrics for query planning/analysis time
spark.sparkContext._jvm.org.apache.spark.sql.catalyst.rules.RuleExecutor.dumpTimeSpent()

# Output to HDFS
df23.write.csv('output')
