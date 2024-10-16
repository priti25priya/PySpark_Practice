from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("CreateAndWriteCSV").getOrCreate()

# Create a DataFrame manually
data = [("a", 55), ("b", 56), ("c", 57)]
columns = ["letter", "number"]
df = spark.createDataFrame(data, schema=columns)

# Show the DataFrame content
df.show()

# Add new constant column
from pyspark.sql.functions import lit
df.withColumn("bonus_percent", lit(0.3)) \
  .show()
# Add New column with NULL
df.withColumn("DEFAULT_COL", lit(None)) \
  .show()
