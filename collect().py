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
df.printSchema()

# Rename the columns
df_renamed = df.withColumnRenamed("letter", "alphabet").withColumnRenamed("number", "value")

# Collect the rows into a list
collected_data = df_renamed.collect()

# Print each row from the collected data
print("\nCollected Data:")
for row in collected_data:
    print(row)
