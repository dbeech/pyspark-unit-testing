# def derive_new_dataframe(df0):
#   df1 = df0.select('*',
#     when(col("col_a") == "aaa", "AAA").otherwise("BBB").alias("col_d"),
#     lit(-99).alias("col_e"),
#     length("col_b").alias("col_f")
#     lit("frmbq").alias("col_g"),
#     lit("qwjlc").alias("col_h"),
#     lit("xmzgs").alias("col_i"),
#     lit("htlof").alias("col_j"),
#     lit("evass").alias("col_k"),
#     lit("hvrmy").alias("col_l"),
#     lit("svswp").alias("col_m"),
#     lit("lfzdm").alias("col_n"),
#     lit("nkmpn").alias("col_o"),
#     lit("canun").alias("col_p"),
#     lit("ibrfa").alias("col_q"),
#     lit("icxyf").alias("col_r"),
#     lit("tonhp").alias("col_s"),
#     lit("ikggy").alias("col_t"),
#     lit("wedtt").alias("col_u"),
#     lit("phhhs").alias("col_v"),
#     lit("tpvzm").alias("col_w"),
#     lit("tehyp").alias("col_x"),
#     lit("lnaae").alias("col_y"),
#     lit("jdwfn").alias("col_z")
#   )
#   return df1

import string
import functions

if __name__ == '__main__':
    for i in range(6,26):
      c = "col_" + string.ascii_letters[i]
      val = functions.random_string(5)
      print("df{} = df{}.withColumn(\"{}\", lit(\"{}\"))".format(i-2, i-3, c, val))
      print("  lit(\"{}\").alias(\"{}\")".format(val,c))