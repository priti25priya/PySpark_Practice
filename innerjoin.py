
from pyspark.python.pyspark.shell import sc

rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("a", 4), ("b", 5), ("d", 6)])
# Perform inner join
inner_join_rdd = rdd1.join(rdd2)

# Show the result
print("Inner Join Result:", inner_join_rdd.collect())
