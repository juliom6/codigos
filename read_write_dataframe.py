# read dataframe twice
df1 = spark.read.parquet(
    root_path + "/bronze/parquet_files/"
)

df2 = spark.read.parquet(
    root_path + "/bronze/parquet_files/"
)

# make a simple join
NUM_ROWS = df1.count()
df_result = df1.join(df2, df1.id + df2.id == (NUM_ROWS + 1)).select(df1["*"])

# write result in parquet format
df_result.write.mode("overwrite").parquet(
    root_path + "/silver/datatool/parquet/"
)

# write result in delta format
df_result.write.format("delta").mode("overwrite").save(
    root_path + "/silver/datatool/delta/"
)

df_result.limit(10).show()
