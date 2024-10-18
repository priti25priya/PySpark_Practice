from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, sum, count

# Create a Spark session
spark = SparkSession.builder.appName("GroupByAggExample").getOrCreate()

# Sample data
data = [
    ("Alice", "Sales", 34),
    ("Bob", "Marketing", 45),
    ("Cathy", "Sales", 29),
    ("David", "IT", 41),
    ("Hammy", "IT", 28),
    ("Eva", "Sales", 38)
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Department", "Age"])

# Group by Department and perform multiple aggregations
df_grouped = df.groupBy("Department").agg(
    count("Name").alias("EmployeeCount"),  # Count the number of employees in each department
    avg("Age").alias("AverageAge"),        # Calculate the average age in each department
    sum("Age").alias("TotalAge")           # Calculate the sum of ages in each department
)
# Show the result
df_grouped.show()
