from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, rank, dense_rank, lag, lead

from windows_dataframe import windowSpec

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

simpleData = (("James", "Sales", 3000),
              ("Michael", "Sales", 4600),
              ("Robert", "Sales", 4100),
              ("Maria", "Finance", 3000),
              ("James", "Sales", 3000),
              ("Scott", "Finance", 3300),
              ("Jen", "Finance", 3900),
              ("Jeff", "Marketing", 3000),
              ("Hammy", "Marketing", 2000),
              ("Olivia", "Sales", 4100)
              )

columns = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema=columns)
df.printSchema()
df.show(truncate=False)

# rank()
df.withColumn("rank", rank().over(windowSpec)).show()
# dense_rank()
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()
# lag()
df.withColumn("lag", lag("salary", 2).over(windowSpec)).show()
# lead()
df.withColumn("lead", lead("salary", 2).over(windowSpec)).show()
