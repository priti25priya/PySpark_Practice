# Select specific columns
from datatype import df

df.select("Name").show()

# Select multiple columns
df.select("Name", "Age").show()
