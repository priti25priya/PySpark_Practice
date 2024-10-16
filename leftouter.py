
from pyspark.python.pyspark.shell import sc

rdd1 = sc.parallelize([("a", 1), ("b", 2), ("c", 3)])
rdd2 = sc.parallelize([("a", 4), ("b", 5), ("d", 6)])
# Perform left outer join
left_outer_join_rdd = rdd1.leftOuterJoin(rdd2)

# Show the result
print("Left Outer Join Result:", left_outer_join_rdd.collect())
