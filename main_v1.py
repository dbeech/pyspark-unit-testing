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

# Derive new columns
df1 = df0.withColumn("col_d", 
                       when(col("col_a") == "aaa", "AAA")
                       .otherwise("BBB"))
df2 = df1.withColumn("col_e", lit(-99))
df3 = df2.withColumn("col_f", length("col_a"))

# Output to HDFS
df3.write.csv('output')