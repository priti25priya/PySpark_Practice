from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("SortExample").getOrCreate()

# Sample data
data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Cathy", 29),
    ("David", 41)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Sort the DataFrame by Age in ascending order
df_sorted_asc = df.sort("Age")
df_sorted_asc.show()

# Sort the DataFrame by Age in descending order
df_sorted_desc = df.sort(df["Age"].desc())
df_sorted_desc.show()
