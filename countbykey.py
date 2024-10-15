# Create an RDD with key-value pairs
from rdd import spark

rdd = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 2), ("b", 3)])

# Count occurrences of each key in the RDD
count_by_key = rdd.countByKey()

# Print the count by key
print(count_by_key)
