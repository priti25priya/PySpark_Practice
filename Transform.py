from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# Create a Spark session
spark = SparkSession.builder.appName("SimpleInterestTransform").getOrCreate()

# Sample DataFrame with Principal, Time, and an array of Rates
data = [
    (1000.0, 2, [5.0, 6.0, 7.0], "Loan for education"),
    (1500.0, 3, [4.5, 5.5], "Personal loan"),
    (2000.0, 1, [3.5, 4.0, 5.0], None)
]

# Define the schema
schema = ["Principal", "Time", "Rates", "Description"]

# Create DataFrame
df = spark.createDataFrame(data, schema=schema)

print("Original DataFrame:")
df.display()

# Use the transform function to calculate Simple Interest for one rate in the Rates array
def ir(df):
  return df.withColumn('ir', df.Principal*df.Time*df.Rates[0]/100)

rd = df.transform(ir).display()
# Use the transform function to calculate Simple Interest for each rate in the Rates array
transformed_df = df.withColumn(
    "SimpleInterest",
    expr("transform(Rates, k -> (Principal * k * Time) / 100)")
)

print("DataFrame after applying the transform function for Simple Interest:")
transformed_df.display()
