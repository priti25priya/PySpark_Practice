# Create two RDDs
from rdd import spark

rdd1 = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
rdd2 = spark.sparkContext.parallelize([4, 5, 6, 7, 8])

# Apply intersection to find common elements
intersection_rdd = rdd1.intersection(rdd2)

# Collect and show the results
print(intersection_rdd.collect())
