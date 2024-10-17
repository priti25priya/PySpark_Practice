# Convert DataFrame to RDD
from dataframes import df

rdd_from_df = df.rdd

# Show RDD
print(rdd_from_df.collect())
