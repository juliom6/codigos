# read csv file
df = spark.read.csv(root_path + "/numbers.csv", header=True)

# write result in parquet format
df.write.mode("overwrite").parquet(
    root_path + "/numbers/"
)
