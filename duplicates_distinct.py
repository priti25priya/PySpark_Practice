from pyspark.sql import SparkSession

# Sample SparkSession
spark = SparkSession.builder.appName("DropDuplicatesExample").getOrCreate()

# Sample DataFrame
data = [
    (1, "Alice", 25,"Finance","Los Angeles"),
    (2, "Bob", 30,"Sales","Los Angeles"),
    (3, "Alice", 25,"Finance","Los Angeles"),
    (4, "David", 40,"Finance","NYC"),
    (5, "Bob", 30,"Sales","Pune"),
    (6, "Hammy",32,"IT","Tokyo"),
    (7,"Olivia",27,"HR","South Korea"),
    (8,"Rio",26,"Engineering","Chicago")
]
columns = ["ID", "Name", "Age","Department","City"]

df = spark.createDataFrame(data, columns)

# Drop duplicates based on the 'Name','Department' and 'Age' columns
df_no_duplicates = df.dropDuplicates(["Name","Age","Department"])

df_no_duplicates.show()

# Drop duplicates considering all columns
df_no_duplicates_all = df.dropDuplicates()

df_no_duplicates_all.show()

# Using distinct() function
distinct_df = df.distinct()
distinct_df.show()


