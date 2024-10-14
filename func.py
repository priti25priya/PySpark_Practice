from pyspark.shell import spark
from pyspark.sql.functions import *


data=[(' Rex',25),('Babu ',21),('Ram',22)]

df=spark.createDataFrame(data=data , schema=['Name','Age'])

# select **********************
"""res=df.select(df['Name'])
res.show()"""

#like*******************
"""
res=df.filter((df['Age']>21) & (df['Name'].like('R%')))
res.show() """

#alias*****************
"""
res = df.select(df["Name"].alias("EmployeeName"))
res.show() """