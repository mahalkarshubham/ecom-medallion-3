# Ensure order_date is not in the future and follows a valid chronological sequence.

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, current_timestamp

spark = (SparkSession.builder.appName("RemoveFutureOrders").getOrCreate())

file_path = r"/Users/shubham/PycharmProjects/ecom-medallion-3/orders.csv"

df = spark.read.csv(file_path, inferSchema=True, header=True)

df = df.withColumn("order_timestamp", to_timestamp("order_timestamp"))

df_filtered = df.filter(col("order_timestamp") <= current_timestamp())

sorted_df = df_filtered.orderBy("order_timestamp")
sorted_df.show()