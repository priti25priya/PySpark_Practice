# Create an RDD using range
from rdd import spark

rdd = spark.sparkContext.range(1, 10)  # Create RDD with numbers from 1 to 9

# Collect and show the results
range_data = rdd.collect()

# Print the range data
print(range_data)
