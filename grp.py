from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("GroupByExample").getOrCreate()

# Sample data
data = [
    ("Alice", "Sales", 34),
    ("Bob", "Marketing", 45),
    ("Cathy", "Sales", 29),
    ("David", "IT", 41),
    ("Hammy", "IT", 25),
    ("Eva", "Sales", 38)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Department", "Age"])

# Group by Department and count the number of employees in each department
df_grouped_count = df.groupBy("Department").count()
df_grouped_count.show()

# Group by Department and calculate the average age in each department
df_grouped_avg = df.groupBy("Department").avg("Age")
df_grouped_avg.show()

# Group by Department and calculate the sum of ages in each department
df_grouped_sum = df.groupBy("Department").sum("Age")
df_grouped_sum.show()
