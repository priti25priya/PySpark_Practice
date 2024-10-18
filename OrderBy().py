from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("OrderByExample").getOrCreate()

# Sample data
data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Cathy", 29),
    ("David", 41),
    ("Hammy", 0),
    (None, 1)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Order the DataFrame by Age in ascending order
df_sorted_asc = df.orderBy("Age")
df_sorted_asc.show()

# Order the DataFrame by Age in descending order
df_sorted_desc = df.orderBy(df["Age"].desc())
df_sorted_desc.show()
