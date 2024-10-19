from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Drop Nulls").getOrCreate()

# Sample data with nulls
data = [
    ("Alice", 30, None),
    ("Bob", None, 50000),
    (None, 25, 60000),
    ("David", 35, None)
]

# Creating DataFrame with sample data
df = spark.createDataFrame(data, ["name", "age", "salary"])

# Dropping rows where any column is null
dropped_df = df.na.drop()

# Show the DataFrame after dropping nulls
dropped_df.show()

# Dropping rows where only 'name' and 'age' columns have nulls
dropped_specific_df = df.na.drop(subset=["name", "age"])

# Show the DataFrame after dropping rows based on specific columns
dropped_specific_df.show()
