from pyspark.sql import SparkSession
from pyspark.sql.functions import array_remove

# Initialize SparkSession
spark = SparkSession.builder.appName("ArrayFunctions").getOrCreate()

# Sample data
data = [
    (1, [1, 2, 3, 4, 3]),
    (2, [5, 6, 7, 5]),
    (3, [8, 9, 10, 8, 11]),
    (4, [13, 13])
]

# Creating a DataFrame
df = spark.createDataFrame(data, ["id", "numbers"])

# Using array_remove to remove all occurrences of a specified value (e.g., 3)
df_with_removed = df.withColumn("array_without_3", array_remove(df["numbers"], 3))

# Show the resulting DataFrame
df_with_removed.show(truncate=False)
