# Read the CSV file
from pyspark.python.pyspark.shell import spark

df2 = spark.read.option("header", "true").option("inferSchema", "true").csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.csv")

# Convert each row to JSON and show the first 10 rows
json_rdd = df2.toJSON()

# Collect and print the JSON-formatted rows
for row in json_rdd.take(10):
    print(row)
