#Aggregate functions
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs,mean,collect_list,collect_set,countDistinct,count,first,last

# Initialize SparkSession
spark = SparkSession.builder.appName("NumericFunctionsExample").getOrCreate()

# Sample DataFrame
simpleData = [("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kate", "Marketing", 2000),
    ("Hammy", "Sales", 4100)
  ]
schema = ["employee_name", "department", "salary"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.printSchema()
df.show(truncate=False)
#mean
df.select(mean("salary")).show(truncate=False)
#avg
print("avg: " + str(df.select(avg("salary")).collect()[0][0]))
#collect_list
df.select(collect_list("salary")).show(truncate=False)
#collect_set
df.select(collect_set("salary")).show(truncate=False)
#countDistinct
df2 = df.select(countDistinct("department", "salary"))
df2.show(truncate=False)
print("Distinct Count of Department & Salary: "+str(df2.collect()[0][0]))
#count
print("count: "+str(df.select(count("salary")).collect()[0]))
#first
df.select(first("salary")).show(truncate=False)
#last
df.select(last("salary")).show(truncate=False)
