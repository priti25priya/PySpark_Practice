from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from sparksession import spark

# Create a DataFrame with some data
df = spark.createDataFrame([
    (1, "Cheng", 100, "2022-01-01"),
    (2, "Harry", 200, "2022-01-02"),
    (3, "Fabrice", 300, "2022-01-03"),
    (4, "Olivia", 400, "2022-01-04"),
    (5, "Max", 500, "2022-01-05")
], ["id", "customer", "amount", "order_date"])

# 1. COUNT(): Count the number of rows in the DataFrame
row_count = df.count()
print(f"Number of rows: {row_count}")

# 2. SELECT(): Select specific columns from the DataFrame
selected_data = df.select("customer", "amount")
selected_data.show()

# 3. FILTER(): Filter the DataFrame where the amount is greater than 200
filtered_data = df.filter(col("amount") > 200)
filtered_data.show()

# 4. WHERE(): Another way to filter, using where() instead of filter()
where_data = df.where(col("amount") > 200)
where_data.show()

# 5. LIKE(): Filter rows where the customer's name starts with 'M'
like_data = df.filter(col("customer").like("M%"))
like_data.show()

# 6. DESCRIBE(): Get summary statistics of the DataFrame
df.describe().show()

# 7. COLUMNS(): List all column names
column_names = df.columns
print(f"Column names: {column_names}")

# 8. WHEN() and 9. OTHERWISE(): Add a new column based on a condition
df_with_flag = df.withColumn("high_amount_flag", when(col("amount") > 300, "Yes").otherwise("No"))
df_with_flag.show()

# 10. ALIAS(): Rename a column using alias
aliased_data = df.select(col("amount").alias("transaction_amount"))
aliased_data.show()
