# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Take the first 3 elements from the RDD
taken_data = rdd.take(3)

# Print the taken data
print(taken_data)
