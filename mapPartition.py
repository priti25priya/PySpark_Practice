# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5], 2)  # 2 partitions

# Apply mapPartitions to sum the elements in each partition
def sum_partition(iterator):
    yield sum(iterator)

mapped_partitions_rdd = rdd.mapPartitions(sum_partition)

# Collect and show the results
print(mapped_partitions_rdd.collect())
