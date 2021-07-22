from pyspark.sql import SparkSession

from pyspark.sql.functions import *

schema = "`date` STRING, `delay` INT, `distance` INT,`origin` STRING, `destination` STRING"

spark = (SparkSession
         .builder
         .appName("departuredelays")
         .getOrCreate()
         )

file = "/home/jaanhunzai_512/spark_projects/data/departuredelays.csv"

flight_df = spark.read.csv(file, schema=schema, header=True)
flight_df.show(n=10, truncate=False)

print(flight_df.printSchema())

new_df = (flight_df.withColumn("flightDate", to_timestamp(col("date"), format="MMddhhmm"))
          .drop("date"))
print(new_df.printSchema())
new_df.show(n=10, truncate=False)

flight_df.createTempView("flightdelays")
"""
- sql query to created view 
"""
spark.sql("""SELECT distance, origin, destination
FROM flightdelays WHERE distance > 1000
ORDER BY distance DESC""").show(10)

"""
find all flights between San Francisco (SFO) and Chicago
(ORD) with at least a two-hour delay
"""
spark.sql("""SELECT date, delay, origin, destination
FROM flightdelays
WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
ORDER by delay DESC""").show(10)

"""
complicated query where we use the CASE clause in SQL. In the following
example, we want to label all US flights, regardless of origin and destination,
with an indication of the delays they experienced: Very Long Delays (> 6 hours),
Long Delays (26 hours)
"""
print("--------flight status --------------")
spark.sql("""SELECT delay, origin, destination,
CASE
WHEN delay > 360 THEN 'Very Long Delays'
WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
WHEN delay = 0 THEN 'No Delays'
ELSE 'Early'
END AS Flight_Delays
FROM flightdelays
ORDER BY origin, delay DESC""").show(10)

# get hightly distance flights 
(new_df.select("distance", "origin", "destination")
.where(col("distance") > 1000)
.orderBy(desc("distance"))).show(10)