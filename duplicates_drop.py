from pyspark.python.pyspark.shell import spark
from pyspark.sql import Row


# Sample data frame
sample_data = [
    Row(id=1, name="John", city="New York"),
    Row(id=2, name="Anna", city="Los Angeles"),
    Row(id=3, name="Mike", city="Chicago"),
    Row(id=1, name="John", city="New York"),
    Row(id=4, name="Sara", city="Houston"),
    Row(id=2, name="Anna", city="Los Angeles")
]

df = spark.createDataFrame(sample_data)

df.show()
# Removing duplicates using drop() function
cleaned_df = df.dropDuplicates()
cleaned_df.show()
# Removing duplicates using specific columns
cleaned_df_id = df.dropDuplicates(["id"])
cleaned_df_id.show()