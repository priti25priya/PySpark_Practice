import pyspark
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
#Create spark session
data = [("Cherry",1000,"USA"), ("Carrots",1500,"USA"), (None,1600,"USA"), \
      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Cherry",400,"China"), \
      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"),("Cherry",1000,"USA"),\
      ("Cherry",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

columns= ["Product","Amount","Country"]
df = spark.createDataFrame(data = data, schema = columns)
df.printSchema()
df.display()


# Applying pivot()
pivotDF = df.groupBy("Country").pivot("Product").sum("Amount")
pivotDF.printSchema()
pivotDF.display()