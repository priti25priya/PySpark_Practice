from pyspark.python.pyspark.shell import sc

rdd1 = sc.parallelize([("a",1), ("b",2), ("c", 3)])
rdd2 = sc.parallelize([("a",9), ("b",6), ("c", 5)])
# Perform right outer join
right_outer_join_rdd = rdd1.rightOuterJoin(rdd2)

# Show the result
print("Right Outer Join Result:", right_outer_join_rdd.collect())

