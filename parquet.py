from pyspark.sql import SparkSession

# Initialize Spark session (this is usually done automatically in Databricks)
spark = SparkSession.builder.appName("CSV to Parquet Conversion").getOrCreate()

# File path for the CSV file
csv_file_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.csv"

# Read the CSV file into a DataFrame
df = spark.read.csv(csv_file_path, header=True, inferSchema=True)

# Show the DataFrame content
df.show(truncate=False)

# File path to save the Parquet file
parquet_file_path = "dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.parquet"

# Write the DataFrame to Parquet format
df.write.parquet(parquet_file_path, mode="overwrite")

# Read the Parquet file into a new DataFrame
parquet_df = spark.read.parquet(parquet_file_path)

# Show the Parquet DataFrame content
parquet_df.show(truncate=False)

# Print schema of the Parquet DataFrame
parquet_df.printSchema()
