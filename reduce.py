# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Use reduce to sum all elements in the RDD
sum_value = rdd.reduce(lambda x, y: x + y)

# Print the sum value
print(sum_value)
