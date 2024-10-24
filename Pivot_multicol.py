from pyspark.sql import SparkSession

# Create a Spark session (if not already created)
spark = SparkSession.builder.appName("MultiplePivotExample").getOrCreate()

# Sample DataFrame
data = [
    ("A", "2024-01", "Sales", 100),
    ("A", "2024-01", "Profit", 20),
    ("A", "2024-02", "Sales", 150),
    ("A", "2024-02", "Profit", 30),
    ("B", "2024-01", "Sales", 200),
    ("B", "2024-01", "Profit", 40),
    ("B", "2024-02", "Sales", 250),
    ("B", "2024-02", "Profit", 50)
]

columns = ["Region", "Month", "Metric", "Value"]

df = spark.createDataFrame(data, schema=columns)

# Show the original DataFrame
print("Original DataFrame:")
df.display()

# Pivot on the first column: 'Metric'
pivot_df = df.groupBy("Region", "Month").pivot("Metric").sum("Value")

# Show the DataFrame after the first pivot
print("DataFrame after first pivot (on 'Metric'):")
pivot_df.display()

# Pivot on the second column: 'Month'
final_df = pivot_df.groupBy("Region").pivot("Month").sum("Sales", "Profit")

# Show the final DataFrame after the second pivot
print("DataFrame after second pivot (on 'Month'):")
final_df.display()
