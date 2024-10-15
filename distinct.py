# Create an RDD with duplicate elements
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 2, 3, 4, 4, 5, 10,10,100,100,20,20,20,30])

# Apply distinct to remove duplicates
distinct_rdd = rdd.distinct()

# Collect and show the results
print(distinct_rdd.collect())
