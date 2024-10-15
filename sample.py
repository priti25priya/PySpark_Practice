#without replacement
# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Apply sample to take a 50% sample without replacement
sampled_rdd = rdd.sample(False, 0.5)

# Collect and show the results
print(sampled_rdd.collect())


#with replacement
# Create an RDD
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

# Apply sample to take a 50% sample with replacement
sampled_rdd_with_replacement = rdd.sample(True, 0.5)

# Collect and show the results
print(sampled_rdd_with_replacement.collect())

