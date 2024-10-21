from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Create SparkSession
spark = SparkSession.builder \
            .appName('SparkByExamples.com') \
            .getOrCreate()
data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
df=spark.createDataFrame(data,["id","input"])
df.show()

#current_date()
df.select(current_date().alias("current_date")
  ).show(1)
#current_timestamp()
data=[["1","02-01-2020 11 01 19 06"],["2","03-01-2019 12 01 19 406"],["3","03-01-2021 12 01 19 406"]]
df2=spark.createDataFrame(data,["id","input"])
df2.show(truncate=False)

df2.select(current_timestamp().alias("current_timestamp")#yyyy-MM-dd HH:mm:ss
  ).show(1,truncate=False)

#date_add()
#add_months() , date_add(), date_sub()
df.select(col("input"),
    add_months(col("input"),3).alias("add_months"),
    add_months(col("input"),-3).alias("sub_months"),
    date_add(col("input"),4).alias("date_add"),
    date_sub(col("input"),4).alias("date_sub")
  ).show()
#datediff()
df.select(col("input"),
    datediff(current_date(),col("input")).alias("datediff")
  ).show()
#year()
#month()
#day()
df.select(col("input"),
     year(col("input")).alias("year"),
     month(col("input")).alias("month"),
     next_day(col("input"),"Sunday").alias("next_day"),
     weekofyear(col("input")).alias("weekofyear")
  ).show()

#to_date()
df.select(col("input"),
    to_date(col("input"), "yyy-MM-dd").alias("to_date")
  ).show()
#date_format()
df.select(col("input"),
    date_format(col("input"), "MM-dd-yyyy").alias("date_format")
  ).show()
#
