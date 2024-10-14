from pyspark.sql.types import StructType,StructField, StringType

from emptyrdd import emptyRDD, spark

schema = StructType([
  StructField('firstname', StringType(), True),
  StructField('middlename', StringType(), True),
  StructField('lastname', StringType(), True)
  ])
df = spark.createDataFrame(emptyRDD,schema)
df.printSchema()