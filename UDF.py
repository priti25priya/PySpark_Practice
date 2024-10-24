from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["Seqno","Name"]
data = [("1", "kim jake"),
    ("2", "tracey smith"),
    ("3", "amy sanders"),
    ("4","None")]

df = spark.createDataFrame(data=data,schema=columns)

df.show(truncate=False)
#Create a Python Function
def convertCase(str):
    if str is None: #handle the null value
        return None
    resStr=""
    arr = str.split(" ")
    for x in arr:
       resStr= resStr + x[0:1].upper() + x[1:len(x)] + " "
    return resStr

# Convert the Python function to PySpark UDF
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

# Converting function to UDF
convertUDF = udf(lambda z: convertCase(z),StringType())
df.select(convertUDF(df.Name).alias("U.name")).display()

# Using UDF with DataFrame
df.select(col("Seqno"),
    convertUDF(col("Name")).alias("Name") ).display()