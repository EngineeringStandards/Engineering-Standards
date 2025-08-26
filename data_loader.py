df = spark.read.csv("/path/to/your/data.csv", header=True, inferSchema=True)
df.write.format("delta").mode("overwrite").saveAsTable("main_app_delta_table")

def get_data():
  return spark.sql("SELECT * FROM main_app_delta_table")