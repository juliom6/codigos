from pyspark.sql import functions as F
from datetime import datetime

NUM_COLUMNS = 10 # 510 limit
NUM_ROWS = (10**5)

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
for col_name in new_columns:
    df = df.withColumn(col_name, F.rand(seed=0))

df.write.mode("overwrite").parquet(
    root_path + "/bronze/test2/"
)
print("Write ok")
print(datetime.now())

df1 = spark.read.parquet(
    root_path + "/bronze/test2/"
)
df2 = spark.read.parquet(
    root_path + "/bronze/test2/"
)
df_result = df1.join(df2, df1.id + df2.id == (NUM_ROWS + 1)).select(df1["*"])
df_result.write.mode("overwrite").parquet(
    root_path + "/bronze/test3/"
)
print(datetime.now())
