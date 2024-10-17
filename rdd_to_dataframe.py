from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create SparkSession
spark = SparkSession.builder.appName("ConversionExample").getOrCreate()

# Example RDD
rdd = spark.sparkContext.parallelize([
    Row(name="Alice", age=30),
    Row(name="Bob", age=25)
])

# Convert RDD to DataFrame
df = rdd.toDF()

# Show DataFrame
df.show()
