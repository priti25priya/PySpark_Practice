from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CollectExample").getOrCreate()

# Create a DataFrame manually
data = [("a", 55), ("b", 56), ("c", 57)]
columns = ["letter", "number"]
df = spark.createDataFrame(data, schema=columns)

# Show the original DataFrame and schema
print("Original DataFrame:")
df.show()

