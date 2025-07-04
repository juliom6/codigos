from pyspark.sql import functions as F
from datetime import datetime

NUM_COLUMNS = 100 # 510 limit
NUM_ROWS = (10**3) * 2 # (10**5) * 2 (9 min) 

###
# SECRET_ACCESS_KEY = ""
# STORAGE_NAME = "xyz123456789"
# spark.sparkContext._jsc.hadoopConfiguration().set(
#     f"fs.azure.account.key.{STORAGE_NAME}.dfs.core.windows.net", SECRET_ACCESS_KEY
# )
# root_path = "abfss://testcontainer@xyz123456789.dfs.core.windows.net"
###

df = spark.range(1, NUM_ROWS + 1)
new_columns = ["col" + ("000" + str(l))[-3:] for l in range(NUM_COLUMNS)]
seed = 0
for col_name in new_columns:
    df = df.withColumn(col_name, F.rand(seed=seed))
    seed += 1

df.repartition(4).write.mode("overwrite").parquet(
    root_path + "/bronze/test1/"
)
print("Write ok")
print(datetime.now())

df1 = spark.read.parquet(
    root_path + "/bronze/test1/"
)
df2 = spark.read.parquet(
    root_path + "/bronze/test1/"
)
df_result = df1.join(df2, df1.id + df2.id == (NUM_ROWS + 1)).select(df1["*"])
df_result.repartition(4).write.mode("overwrite").parquet(
    root_path + "/bronze/test2/"
)
print(datetime.now())
df_result.show()
