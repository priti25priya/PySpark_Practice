from pyspark.shell import spark
from pyspark.sql.functions import *

data = [("Hammy", "HR", 4000),
        ("Sarah", "Finance", 6500),
        ("Mike", "Finance", 4000),
        ("Olivia", "HR", 3500),
        ("Saga", "IT", 9000),
        ("James", "IT", 5500)]

columns = ["Name", "Department", "Salary"]

df=spark.createDataFrame(data,columns)

res=df.groupBy("Department").agg(
    avg("Salary").alias("AverageSalary"),
    sum("Salary").alias("TotalSalary"),
    max("Salary").alias("Max_Salary"),
)

final_res=res.orderBy(res['Max_Salary'].desc())

final_res.show()