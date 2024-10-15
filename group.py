# Create an RDD with key-value pairs
from rdd import spark

rdd = spark.sparkContext.parallelize([("a", 1), ("b", 1), ("a", 2), ("b", 3), ("c", 1)])

# Apply groupByKey to group values by key
grouped_rdd = rdd.groupByKey()

# Collect and show the results as lists
print([(k, list(v)) for k, v in grouped_rdd.collect()])
