# Create two RDDs with the same number of elements and partitions
from rdd import spark

rdd1 = spark.sparkContext.parallelize(["Red", "Tasty", "Awesome"])
rdd2 = spark.sparkContext.parallelize(['cherry', 'apple', 'developers'])

# Apply zip to combine them
zipped_rdd = rdd1.zip(rdd2)

# Collect and show the results
print(zipped_rdd.collect())
