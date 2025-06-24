table_path = root_path + "/RAW/test/"
df = spark.range(1000)
df.write.format("delta").mode("overwrite").save(table_path + "tabla_delta")
spark.read.format("delta").load(table_path + "tabla_delta").show()