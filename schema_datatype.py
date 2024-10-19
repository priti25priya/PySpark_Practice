from pyspark.sql.types import StructType, StructField, ArrayType, StringType, LongType, IntegerType, BooleanType
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("DataType Example").getOrCreate()

# Sample schema definition using various data types
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("salary", LongType(), True),
    StructField("is_manager", BooleanType(), True),
    StructField("skills", ArrayType(StringType()), True),
    StructField("address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("zipcode", LongType(), True)
    ]), True)
])

# Sample data
data = [
    ("Alice", 30, 100000, True, ["Python", "Spark"], {"street": "123 Main St", "city": "New York", "zipcode": 10001}),
    ("Bob", 25, 80000, False, ["Java", "Hadoop"], {"street": "456 Maple Dr", "city": "San Francisco", "zipcode": 94107}),
    ("Charlie", 35, 120000, True, ["Scala", "Kafka"], {"street": "789 Broadway", "city": "Seattle", "zipcode": 98101})
]

# Create DataFrame with the defined schema
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show(truncate=False)

# Print schema
df.printSchema()
