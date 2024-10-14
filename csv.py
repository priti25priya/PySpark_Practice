# Read a CSV file into a DataFrame
from emptyrdd import spark

df_csv = spark.read.csv("dbfs:/FileStore/shared_uploads/priti25priya@gmail.com/data.csv", header=True)

# Show DataFrame
df_csv.count()


def display(param):
    pass


display(df_csv.limit(100))



