from pyspark.python.pyspark.shell import spark

data={('IT', 8, 5),
      ('Payroll', 3, 2),
      ('HR', 2, 4)
      }
schema=['dep','male','female']
df=spark.createDataFrame(data,schema)
df.display()

from pyspark.sql.functions import expr
unpivotDf = df.select('dep', expr("stack(2, 'male', male,'female',female) as (gender,ct)"))
unpivotDf.display()
