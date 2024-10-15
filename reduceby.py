# Create an RDD with key-value pairs
from rdd import spark

rdd = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 2), ("b", 3), ("c", 1)])

# Apply reduceByKey to sum the values for each key
reduced_rdd = rdd.reduceByKey(lambda x, y: x + y)

# Collect and show the results
print(reduced_rdd.collect())
