
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

# 1.Replacing nulls with specific values in individual columns
filled_df = df.fillna({
    "name": "Unknown",   # Replace nulls in 'name' column with 'Unknown'
    "age": 0,            # Replace nulls in 'age' column with 0
    "salary": 30000      # Replace nulls in 'salary' column with 30000
})

# Show the DataFrame after filling nulls
filled_df.show()

# Replace all null numeric values with 0
# 2.Using fillna() to Replace All Null Values in a DataFrame
filled_numeric_df = df.fillna(0)

# Replace all null string values with 'Unknown'
filled_string_df = df.fillna("Unknown")

# Show the DataFrames after replacing nulls
filled_numeric_df.show()
filled_string_df.show()

