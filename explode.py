import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer

# Initialize SparkSession
spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

# Sample data
arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})
]

# Creating a DataFrame
df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])

# Display the schema and the DataFrame content
df.printSchema()
df.show(truncate=False)

# explode() on array column
df2 = df.select(df.name, explode(df.knownLanguages))
df2.printSchema()
df2.show()

# explode() on map column
df3 = df.select(df.name, explode(df.properties))
df3.printSchema()
df3.show()

# explode_outer() on array and map column
df.select(df.name, explode_outer(df.knownLanguages)).show()
df.select(df.name, explode_outer(df.properties)).show()

# posexplode() on array and map
df.select(df.name, posexplode(df.knownLanguages)).show()
df.select(df.name, posexplode(df.properties)).show()

# posexplode_outer() on array and map
df.select(df.name, posexplode_outer(df.knownLanguages)).show()
df.select(df.name, posexplode_outer(df.properties)).show()
