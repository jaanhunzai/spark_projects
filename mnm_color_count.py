import sys
from  pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (SparkSession.builder.appName("pythonColorCount").getOrCreate())
#print(spark)
"""
# Read the file into a Spark DataFrame using the CSV
# format by inferring the schema and specifying that the
# file contains a header, which provides column names for comma-
# separated fields.
"""
mnm_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema","true")\
    .load("/home/jaanhunzai_512/spark_projects/data/mnm_dataset.csv")
#print(mnm_df.head(10))

"""
# We use the DataFrame high-level APIs. Note
# that we don't use RDDs at all. Because some of Spark's
# functions return the same object, we can chain function calls.
# 1. Select from the DataFrame the fields "State", "Color", and "Count"
# 2. Since we want to group each state and its M&M color count,
# we use groupBy()
# 3. Aggregate counts of all colors and groupBy() State and Color
# 4 orderBy() in descending order
"""
count_mnm_df = (mnm_df
.select("State", "Color", "Count")
.groupBy("State", "Color")
.agg(count("Count").alias("Total"))
.orderBy("Total", ascending=False))


count_mnm_df.show(n=60, truncate=False)


with open ("/home/jaanhunzai_512/spark_projects/data/mnm_result.txt", "w") as f:
    f.write(f'number of LINES: {count_mnm_df.count()}\n')
    f.write(f'number of counts for each state: {count_mnm_df.head(count_mnm_df.count())}\n')


from pyspark.sql.functions import avg
"""
high-level DSL operators (domain specific language)
- are used to reduce complexity and performs operations as a single query
"""
spark_sdl =(SparkSession
            .builder
            .appName("spark_DSL_operators")
            .getOrCreate())
sdl_df = spark.createDataFrame([("Jan", "40"),("Khan","60"),("Jan","42"),("Ali","89")],["name","age"])
avg_df = sdl_df.groupby("name").agg(avg("age"))

avg_df.show()