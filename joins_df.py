#JOINS
import pyspark
from pyspark.python.pyspark.shell import spark
from pyspark.sql.functions import col,broadcast

emp = [(1,"Smith",-1,"2018","10","M",3000),
    (2,"Rose",1,"2010","20","M",4000),
    (3,"Williams",1,"2010","10","M",1000),
    (4,"Jones",2,"2005","10","F",2000),
    (5,"Brown",2,"2010","40","",-1),
      (6,"Brown",2,"2010","50","",-1)
  ]
empColumns = ["emp_id","name","superior_emp_id","year_joined",
       "emp_dept_id","gender","salary"]

empDF = spark.createDataFrame(data=emp, schema = empColumns)
empDF.printSchema()
empDF.show(truncate=False)

dept = [("Finance",10),
    ("Marketing",20),
    ("Sales",30),
    ("IT",40)
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.printSchema()
deptDF.show(truncate=False)
# inner join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"inner").show(truncate=False)
# cross join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"cross").show(truncate=False)
# fullouter join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"fullouter").show(truncate=False)
# left/leftouter join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"leftouter").show(truncate=False)
# right/rightouter join
empDF.join(deptDF,empDF.emp_dept_id ==  deptDF.dept_id,"rightouter").show(truncate=False)
# left semi-join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftsemi").show(truncate=False)
#  left anti-join
empDF.join(deptDF, empDF.emp_dept_id == deptDF.dept_id, "leftanti").show(truncate=False)
#  self join
empDF.alias("emp1").join(empDF.alias("emp2"),
    col("emp1.superior_emp_id") == col("emp2.emp_id"),"inner").select(col("emp1.emp_id"),col("emp1.name"),
      col("emp2.emp_id").alias("superior_emp_id"),
      col("emp2.name").alias("superior_emp_name")).show(truncate=False)
#  broadcast join
empDF.join(broadcast(deptDF), empDF.emp_dept_id == deptDF.dept_id, "inner").show(truncate=False)
