# Ensure no mandatory fields (product_id, product_name, category) are null or empty.
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

spark = SparkSession.builder.appName("ValidateMandatoryFields").getOrCreate()

file_path = r"/Users/shubham/PycharmProjects/ecom-medallion-3/orders.csv"

df = spark.read.csv(file_path, inferSchema=True, header=True)

invalid_df = df.filter(
    col("product_id").isNull() | (trim(col("product_id")) == "") |
    col("product_name").isNull() | (trim(col("product_name")) == "") |
    col("category").isNull() | (trim(col("category")) == "")
)
valid_df = df.subtract(invalid_df)

valid_df.show()
