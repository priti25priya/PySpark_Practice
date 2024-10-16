
from pyspark.python.pyspark.shell import sc

rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("a", 4), ("b", 5), ("d", 6)])
# Perform full outer join
full_outer_join_rdd = rdd1.fullOuterJoin(rdd2)

# Show the result
print("Full Outer Join Result:", full_outer_join_rdd.collect())
