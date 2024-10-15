# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 4)  #4 partition

# Apply glom to view elements in each partition
glommed_rdd = rdd.glom()

# Collect and show the results
print(glommed_rdd.collect())
