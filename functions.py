from pyspark.sql.functions import *
import string
import random


def random_string(length=10):
  letters = string.ascii_lowercase
  return ''.join(random.choice(letters) for i in range(length))


def derive_new_dataframe(df0):
  df1 = df0.withColumn("col_d", derive_new_column())
  df2 = df1.withColumn("col_e", lit(-99))
  df3 = df2.withColumn("col_f", length("col_a"))
  df4 = df3.withColumn("col_g", lit("frmbq"))
  df5 = df4.withColumn("col_h", lit("qwjlc"))
  df6 = df5.withColumn("col_i", lit("xmzgs"))
  df7 = df6.withColumn("col_j", lit("htlof"))
  df8 = df7.withColumn("col_k", lit("evass"))
  df9 = df8.withColumn("col_l", lit("hvrmy"))
  df10 = df9.withColumn("col_m", lit("svswp"))
  df11 = df10.withColumn("col_n", lit("lfzdm"))
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
  return df23


def derive_new_column():
  return when(col('col_a') == 'aaa', 'AAA').otherwise('BBB')

