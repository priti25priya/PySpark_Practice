#1. question:

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("get count").getOrCreate()
schema1 = StructType([StructField("customer", StringType(), nullable=False),
                      StructField("product_model", StringType(), nullable=False)
                      ])
data1= [("1", "iphone13"),
         ("1", "dell i5 core"),
         ("2", "iphone13"),
         ("2", "dell i5 core"),
         ("3", "iphone13"),
         ("3", "dell i5 core"),
         ("1", "dell i3 core"),
         ("1", "hp i5 core"),
         ("1", "iphone14"),
         ("3", "iphone14"),
         ("4", "iphone13")]
schema2 = StructType([
    StructField("product_model", StringType(), nullable=False)])
data2 = [("iphone13",),
         ("dell i5 core",),
         ("dell i3 core",),
         ("hp i5 core",),
         ("iphone14",)]
df1 = spark.createDataFrame(data=data1, schema=schema1)
df2 = spark.createDataFrame(data=data2, schema=schema2)
def only_product_iphone13(purchase_data_df):
    return purchase_data_df.groupBy("customer") \
            .agg({"product_model": "collect_set"}) \
            .filter("size(collect_set(product_model)) = 1 and collect_set(product_model)[0] = 'iphone13'").select("customer")


def iphone13_to_iphone14(purchase_data_df):
    iphone13 = purchase_data_df.filter("product_model = 'iphone13'").select("customer")
    iphone14 = purchase_data_df.filter("product_model = 'iphone14'").select("customer")
    return iphone13.intersect(iphone14)

def product_unique(df1,df2):
    count1 = df1.groupBy("customer").agg(countDistinct("product_model").alias("distinct_count"))
    count2 = df2.count()
    result=count1.filter(count1.distinct_count==count2)
    return result

# 2. question:

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Step 1: Create a Spark session
spark = SparkSession.builder \
    .appName("CreditCardMasking") \
    .getOrCreate()

# Step 2: Create DataFrame using different read methods
# Option 1: Using a list of tuples
data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

credit_card_df = spark.createDataFrame(data, ["card_number"])


# Print the original DataFrame
credit_card_df.display()

# Step 2: Print number of partitions
print(f"Original number of partitions: {credit_card_df.rdd.getNumPartitions()}")

# Step 3: Increase the partition size to 5
credit_card_df = credit_card_df.repartition(5)
print(f"Number of partitions after increasing: {credit_card_df.rdd.getNumPartitions()}")

# Step 4: Decrease the partition size back to its original partition size
credit_card_df = credit_card_df.coalesce(1)
print(f"Number of partitions after decreasing: {credit_card_df.rdd.getNumPartitions()}")

# Step 5: Create a UDF to mask credit card numbers
def mask_credit_card(card_number):
    return '*' * (len(card_number) - 4) + card_number[-4:]

mask_udf = udf(mask_credit_card, StringType())

# Step 6: Create a new DataFrame with masked card numbers
masked_card_df = credit_card_df.withColumn("masked_card_number", mask_udf(credit_card_df.card_number))

# Show the final output with original and masked card numbers
masked_card_df.select("card_number", "masked_card_number").display()

# 3. question:

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, current_date, datediff
from datetime import datetime

# Step 1: Create a Spark session
spark = SparkSession.builder \
    .appName("UserActivity") \
    .getOrCreate()

# Step 2: Define the data with datetime conversion
data = [
    (1, 101, 'login', datetime.strptime('2023-09-05 08:30:00', '%Y-%m-%d %H:%M:%S')),
    (2, 102, 'click', datetime.strptime('2023-09-06 12:45:00', '%Y-%m-%d %H:%M:%S')),
    (3, 101, 'click', datetime.strptime('2023-09-07 14:15:00', '%Y-%m-%d %H:%M:%S')),
    (4, 103, 'login', datetime.strptime('2023-09-08 09:00:00', '%Y-%m-%d %H:%M:%S')),
    (5, 102, 'logout', datetime.strptime('2023-09-09 17:30:00', '%Y-%m-%d %H:%M:%S')),
    (6, 101, 'click', datetime.strptime('2023-09-10 11:20:00', '%Y-%m-%d %H:%M:%S')),
    (7, 103, 'click', datetime.strptime('2023-09-11 10:15:00', '%Y-%m-%d %H:%M:%S')),
    (8, 102, 'click', datetime.strptime('2023-09-12 13:10:00', '%Y-%m-%d %H:%M:%S'))
]

# Step 3: Create schema using StructType and StructField
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("user_activity", StringType(), True),
    StructField("time_stamp", TimestampType(), True)
])

# Step 4: Create DataFrame with the schema
user_activity_df = spark.createDataFrame(data, schema)

# Show the initial DataFrame
user_activity_df.show(truncate=False)

# Step 5: Write a dynamic function to rename columns
def rename_columns(df):
    new_columns = ["log_id", "user_id", "user_activity", "time_stamp"]
    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    return df

# Rename columns
user_activity_df = rename_columns(user_activity_df)

# Step 6: Calculate the number of actions performed by each user in the last 7 days
actions_count_df = user_activity_df.filter(
    datediff(current_date(), col("time_stamp")) <= 7
).groupBy("user_id").count().withColumnRenamed("count", "actions_performed")

actions_count_df.show()

# Step 7: Convert the time_stamp column to the login_date column in YYYY-MM-DD format
user_activity_df = user_activity_df.withColumn(
    "login_date", to_date(col("time_stamp"))
).drop("time_stamp")

# Show the updated DataFrame
user_activity_df.show()

# Step 8: Write the DataFrame as a CSV file with different write options
user_activity_df.write.csv("user_activity.csv", mode="overwrite", header=True)

# Step 9: Write as a managed table with the Database name as 'user' and table name as 'login_details'
# Make sure the database 'user' exists
spark.sql("CREATE DATABASE IF NOT EXISTS user")
user_activity_df.write.saveAsTable("user.login_details", mode="overwrite")

