# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions

# Apply mapPartitionsWithIndex to show index and elements
def show_index(index, iterator):
    yield (index, list(iterator))

indexed_rdd = rdd.mapPartitionsWithIndex(show_index)

# Collect and show the results
print(indexed_rdd.collect())
