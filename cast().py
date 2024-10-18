from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, BooleanType, DateType

# Create a Spark session
spark = SparkSession.builder.appName("CastingExample").getOrCreate()

# Sample data
data = [
    ("Alice", "34", "true", "2023-01-01"),
    ("Bob", "45", "false", "2022-05-15"),
    ("Cathy", "29", "true", "2021-12-12"),
    ("David", "41", "false", "2020-07-07")
]

# Create a DataFrame with column names
df = spark.createDataFrame(data, ["Name", "age", "isGraduated", "jobStartDate"])

# Check if the DataFrame has the correct schema
df.printSchema()

# Convert the "age" column to IntegerType
df = df.withColumn("age", df["age"].cast(IntegerType()))

# Convert "isGraduated" to BooleanType
df = df.withColumn("isGraduated", df["isGraduated"].cast(BooleanType()))

# Convert "jobStartDate" to DateType
df = df.withColumn("jobStartDate", df["jobStartDate"].cast(DateType()))

# Show the updated DataFrame
df.show()

# Using select to cast columns
df.select(col("age").cast("int").alias("age")).show()

# Using selectExpr() to cast columns
df.selectExpr("cast(age as int) as age").show()

# Using spark.sql() to cast columns (requires creating a temporary view)
df.createOrReplaceTempView("CastExample")
spark.sql("SELECT CAST(age AS INT), CAST(isGraduated AS BOOLEAN), CAST(jobStartDate AS DATE) FROM CastExample").show()
