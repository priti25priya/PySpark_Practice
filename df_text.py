from pyspark.python.pyspark.shell import spark

df2 = spark.read.text("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.csv")

for i in df2.take(10):
    print(i)
