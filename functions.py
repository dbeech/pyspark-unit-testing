from pyspark.sql.functions import *

def derive_new_dataframe(df0):
  df1 = df0.withColumn("col_d", 
                       when(col("col_a") == "aaa", "AAA")
                       .otherwise("BBB"))
  df2 = df1.withColumn("col_e", lit(-99))
  df3 = df2.withColumn("col_f", length("col_a"))
  return df3


  