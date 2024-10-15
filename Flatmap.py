# Create an RDD
from rdd import spark

rdd = spark.sparkContext.parallelize(["Hello Coders", "Apache Spark","Black Mustang"])

# Apply flatMap to split each line into words
flatmapped_rdd = rdd.flatMap(lambda line: line.split(" "))

# Collect and show the results
print(flatmapped_rdd.collect())
