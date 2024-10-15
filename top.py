# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 3, 5, 2, 4])

# Get the top 3 elements (largest values) from the RDD
top_data = rdd.top(3)

# Print the top data
print(top_data)
