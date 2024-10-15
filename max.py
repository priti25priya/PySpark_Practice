# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 39, 41, 5])

# Find the maximum element in the RDD
max_value = rdd.max()

# Print the maximum value
print(max_value)
