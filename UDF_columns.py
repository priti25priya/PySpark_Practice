from pyspark.sql.connect.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from sparksession import df


def upperCase(str):
    return str.upper()


upperCaseUDF = udf(lambda z: upperCase(z), StringType())

df.withColumn("C.Name", upperCaseUDF(col("Name"))).display()