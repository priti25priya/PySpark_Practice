# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([5, 2, 3, 4, 1])

# Find the minimum element in the RDD
min_value = rdd.min()

# Print the minimum value
print(min_value)
