from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

# Initialize SparkSession
spark = SparkSession.builder.appName("NumericFunctionsExample").getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice", 25, 70000.5),
    (2, "Bob", 30, 85000.0),
    (3, "Charlie", 25, 60000.75),
    (4, "David", 40, 90000.0),
    (5, "Eve", 35, 75000.0),
    (6, "Frank", 28, -50000.0),
    (7, "Grace", 32, 65000.5)
]
columns = ["ID", "Name", "Age", "Salary"]

df = spark.createDataFrame(data, columns)

# Display the original DataFrame
print("Original DataFrame:")
df.show()

# Apply numeric functions
# 1. Calculate the sum of the 'Salary' column
df_sum = df.select(sum("Salary").alias("Total_Salary"))
print("Sum of Salaries:")
df_sum.show()

# 2. Calculate the average salary
df_avg = df.select(avg("Salary").alias("Average_Salary"))
print("Average Salary:")
df_avg.show()

# 3. Find the minimum and maximum salary
df_min_max = df.select(min("Salary").alias("Minimum_Salary"), max("Salary").alias("Maximum_Salary"))
print("Minimum and Maximum Salary:")
df_min_max.show()

# 4. Round the 'Salary' column to 2 decimal places
df_rounded = df.withColumn("Rounded_Salary", round("Salary", 2))
print("Rounded Salary:")
df_rounded.show()

# 5. Get the absolute value of the 'Salary' column
df_abs = df.withColumn("Absolute_Salary", abs("Salary"))
print("Absolute Salary:")
df_abs.show()

# 6. Use AND (&) and OR (|) operations
# Filter where Age is greater than 25 AND Salary is greater than 70000, or Age is less than 30
df_filtered = df.filter((df.Age > 25) & (df.Salary > 70000) | (df.Age < 30))
print("Filtered DataFrame using AND and OR operations:")
df_filtered.show()
