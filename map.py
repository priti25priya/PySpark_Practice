# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Apply map transformation to multiply each element by 2
mapped_rdd = rdd.map(lambda x: x * 2)

# Collect and show the results
print(mapped_rdd.collect())
