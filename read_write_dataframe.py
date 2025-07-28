##### read dataframe twice
df1 = spark.read.parquet(
    root_path + "/bronze/parquet_files/"
)

df2 = spark.read.parquet(
    root_path + "/bronze/parquet_files/"
)

##### make join and write result
NUM_ROWS = df1.count()

df_result = df1.join(df2, df1.id + df2.id == (NUM_ROWS + 1)).select(df1["*"])
df_result.write.mode("overwrite").parquet(
    root_path + "/silver/datatool/"
)

df_result.limit(10).show()
