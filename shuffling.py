from pyspark.sql import SparkSession

from sparksession import spark

# Create a DataFrame with some data
df = spark.createDataFrame([
    (1, "Ahmed", 100, "2022-01-01"),
    (2, "John", 200, "2022-01-02"),
    (3, "Fabrice", 300, "2022-01-03"),
    (4, "Mehdi", 400, "2022-01-04"),
    (5, "Mehdi", 500, "2022-01-05")
], ["id", "customer", "amount", "order_date"])

# Group the data by the "customer" column and sum the amount of each transaction
grouped_data = df.groupBy("customer").sum("amount")

# Print the results
grouped_data.show()
