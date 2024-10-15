# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10,22,89,78,56,45])

# Apply filter to keep only even numbers
filtered_rdd = rdd.filter(lambda x: x % 2 == 0)

# Collect and show the results
print(filtered_rdd.collect())
