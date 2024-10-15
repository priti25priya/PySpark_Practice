from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
# Create RDD from parallelize
data = [1,2,3,4,5,6,7,8,9,10,11,12]
rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())



# Read the CSV file from DBFS
rdd2 = spark.sparkContext.textFile("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.csv")

for i in rdd2.take(10):
    print(i)


