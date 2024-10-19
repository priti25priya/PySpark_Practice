from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("Handling Nulls").getOrCreate()

# Sample data with nulls
data = [
    ("Alice", 30, None),
    ("Bob", None, 50000),
    (None, 25, 60000),
    ("David", 35, None)
]

# Creating DataFrame with sample data
df = spark.createDataFrame(data, ["name", "age", "salary"])

# 1.Filtering out rows where 'name' or 'age' is null
filtered_df = df.filter(col("name").isNotNull() & col("age").isNotNull())

# Show the filtered DataFrame
filtered_df.show()



# 2.Replacing nulls in specific columns
filled_df = df.fillna({
    "name": "Unknown",  # Replace nulls in 'name' with 'Unknown'
    "age": 0,           # Replace nulls in 'age' with 0
    "salary": 30000     # Replace nulls in 'salary' with 30000
})

# Show the DataFrame after replacing nulls
filled_df.show()

from pyspark.sql.functions import coalesce

# 3.Using coalesce to choose the first non-null value between 'salary' and a default value
coalesce_df = df.withColumn("salary", coalesce(col("salary"), col("age") * 1000))

# Show the DataFrame after using coalesce
coalesce_df.show()



