# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Calculate the mean of the elements in the RDD
mean_value = rdd.mean()

# Print the mean value
print(mean_value)
