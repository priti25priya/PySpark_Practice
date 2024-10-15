# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Use fold to sum all elements in the RDD, starting with an initial value of 0
folded_sum = rdd.fold(0, lambda x, y: x + y)

# Print the folded sum
print(folded_sum)
