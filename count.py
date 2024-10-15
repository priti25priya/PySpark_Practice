# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])

# Count the number of elements in the RDD
count_data = rdd.count()

# Print the count
print(count_data)
