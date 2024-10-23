from pyspark.sql import SparkSession
from pyspark.sql.functions import array, array_contains, size, expr

# Initialize SparkSession
spark = SparkSession.builder.appName("ArrayFunctions").getOrCreate()

# Sample data
data = [
    (1, [1, 2, 3, 4]),
    (2, [5, 6, 7]),
    (3, [8, 9, 10, 11, 12]),
    (4, [13])
]

# Creating a DataFrame
df = spark.createDataFrame(data, ["id", "numbers"])

# 1. ARRAY function: Demonstrating creating an array using literals
df_with_array = df.withColumn("new_array", array(df["id"], expr("array(1, 2, 3)")[0]))
df_with_array.show()

# 2. ARRAY_CONTAINS function: Checking if array contains a specific element (e.g., 7)
df_with_contains = df.withColumn("contains_7", array_contains(df["numbers"], 7))
df_with_contains.show()

# 3. ARRAY_LENGTH function: Getting the length of the array
df_with_length = df.withColumn("array_length", size(df["numbers"]))
df_with_length.show()

# 4. ARRAY_POSITION function: Get index of a specific element (e.g., 7)
df_with_pos = df.withColumn("index_of_7", expr("array_position(numbers,7)"))
df_with_pos.show()

