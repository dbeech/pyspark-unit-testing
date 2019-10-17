from pyspark.sql import SparkSession

from functions import derive_new_dataframe

# Delete previous output 
# !hdfs dfs -rm -r -skipTrash output

# Create Spark session
spark = SparkSession \
  .builder \
  .appName("PySpark Testing") \
  .config("spark.driver.memory", "2g") \
  .config("spark.executor.cores", 4) \
  .config("spark.dynamicAllocation.enabled", "true") \
  .config("spark.dynamicAllocation.maxExecutors", 8) \
  .getOrCreate()

# Read input CSV from HDFS
df0 = spark.read.csv('input', header=True)

# Derive new columns
df1 = derive_new_dataframe(df0)

# Output to HDFS
df1.write.csv('output')
