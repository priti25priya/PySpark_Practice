from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("RenameColumns").getOrCreate()

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

# Show the renamed DataFrame and schema
print("\nDataFrame with Renamed Columns:")
df_renamed.show()
df_renamed.printSchema()
