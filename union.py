# Create two RDDs
from rdd import spark

rdd1 = spark.sparkContext.parallelize(["Red", 2, 3])
rdd2 = spark.sparkContext.parallelize([4, "Cherry", 6])

# Apply union to combine the RDDs
union_rdd = rdd1.union(rdd2)

# Collect and show the results
print(union_rdd.collect())
