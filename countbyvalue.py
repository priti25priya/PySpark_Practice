# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 2, 3, 1, 3, 4])

# Count occurrences of each value in the RDD
count_by_value = rdd.countByValue()

# Print the count by value
print(count_by_value)
