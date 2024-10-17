from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from rdd_to_dataframe import spark, rdd

# Define a schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])

# Convert RDD to DataFrame with schema
df_with_schema = spark.createDataFrame(rdd, schema)

# Show DataFrame
df_with_schema.show()
