from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    upper, lower, trim, ltrim, rtrim, translate, substring,
    substring_index, split, repeat, rpad, lpad, regexp_replace,
    regexp_extract, length
)

# Create a Spark session
spark = SparkSession.builder.appName("IndividualStringFunctions").getOrCreate()

# Sample data
data = [
    (" Alice ", "34", "true", "2023-01-01"),
    ("Bob   ", "45", "false", "2022-05-15"),
    ("Cathy", "29", "true", "2021-12-12"),
    ("David", "41", "false", "2020-07-07"),
    (" Eva  ", "38", "true", "2023-08-09")
]

# Create a DataFrame
df = spark.createDataFrame(data, ["Name", "Age", "isGraduated", "jobStartDate"])

# 1. Apply upper() function
df_upper = df.withColumn("upper_name", upper("Name"))
df_upper.show(truncate=False)

# 2. Apply lower() function
df_lower = df.withColumn("lower_name", lower("Name"))
df_lower.show(truncate=False)

# 3. Apply trim() function
df_trimmed = df.withColumn("trimmed_name", trim("Name"))
df_trimmed.show(truncate=False)

# 4. Apply ltrim() function
df_ltrim = df.withColumn("ltrim_name", ltrim("Name"))
df_ltrim.show(truncate=False)

# 5. Apply rtrim() function
df_rtrim = df.withColumn("rtrim_name", rtrim("Name"))
df_rtrim.show(truncate=False)

# 6. Apply translate() function (replace 'a' with 'o')
df_translated = df.withColumn("translated_name", translate("Name", "a", "o"))
df_translated.show(truncate=False)

# 7. Apply substring() function (extract first 3 characters)
df_substring = df.withColumn("sub_name", substring("Name", 1, 3))
df_substring.show(truncate=False)

# 8. Apply substring_index() function (extract substring before 'a')
df_sub_index = df.withColumn("sub_index_name", substring_index("Name", "a", 1))
df_sub_index.show(truncate=False)

# 9. Apply split() function (split based on 'a')
df_split = df.withColumn("split_name", split("Name", "a"))
df_split.show(truncate=False)

# 10. Apply repeat() function (repeat name twice)
df_repeat = df.withColumn("repeated_name", repeat("Name", 2))
df_repeat.show(truncate=False)

# 11. Apply rpad() function (pad with '*' to a length of 10)
df_rpad = df.withColumn("rpad_name", rpad("Name", 10, "*"))
df_rpad.show(truncate=False)

# 12. Apply lpad() function (pad with '*' to a length of 10)
df_lpad = df.withColumn("lpad_name", lpad("Name", 10, "*"))
df_lpad.show(truncate=False)

# 13. Apply regex_replace() function (replace 'a' with 'o')
df_regex_replace = df.withColumn("regex_replace_name", regexp_replace("Name", "a", "o"))
df_regex_replace.show(truncate=False)

# 14. Apply regex_extract() function (extract the first 'a')
df_regex_extract = df.withColumn("regex_extract_name", regexp_extract("Name", "(a)", 1))
df_regex_extract.show(truncate=False)

# 15. Apply length() function
df_length = df.withColumn("name_length", length(trim("Name")))
df_length.show(truncate=False)

