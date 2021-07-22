from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.types import *
from pyspark.sql import *

spark = (SparkSession.builder.appName("df_read_write").getOrCreate())

# define schema
fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
                          StructField('UnitID', StringType(), True),
                          StructField('IncidentNumber', IntegerType(), True),
                          StructField('CallType', StringType(), True),
                          StructField('CallDate', StringType(), True),
                          StructField('WatchDate', StringType(), True),
                          StructField('CallFinalDisposition', StringType(), True),
                          StructField('AvailableDtTm', StringType(), True),
                          StructField('Address', StringType(), True),
                          StructField('City', StringType(), True),
                          StructField('Zipcode', IntegerType(), True),
                          StructField('Battalion', StringType(), True),
                          StructField('StationArea', StringType(), True),
                          StructField('Box', StringType(), True),
                          StructField('OriginalPriority', StringType(), True),
                          StructField('Priority', StringType(), True),
                          StructField('FinalPriority', IntegerType(), True),
                          StructField('ALSUnit', BooleanType(), True),
                          StructField('CallTypeGroup', StringType(), True),
                          StructField('NumAlarms', IntegerType(), True),
                          StructField('UnitType', StringType(), True),
                          StructField('UnitSequenceInCallDispatch', IntegerType(), True),
                          StructField('FirePreventionDistrict', StringType(), True),
                          StructField('SupervisorDistrict', StringType(), True),
                          StructField('Neighborhood', StringType(), True),
                          StructField('Location', StringType(), True),
                          StructField('RowID', StringType(), True),
                          StructField('Delay', FloatType(), True)])


file = "/home/jaanhunzai_512/spark_projects/data/Fire_Incidents.csv"
df = spark.read.csv(file,schema=fire_schema, header=True)
df.show(10)

#print(df.printSchema())
#"saving data as parquet file"
#df.write.format("parquet").save("/home/jaanhunzai_512/spark_projects/data/parquet.parquet")

few_fire_df = (df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
#few_fire_df.show()


from pyspark.sql.functions import *
(df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .agg(countDistinct("CallType").alias("DistinctCallTypes"))
 .show())
# list the distinct call type
(df
 .select("CallType")
 .where(col("CallType").isNotNull())
 .distinct()
 .show(10, False))

"""
- common operations on columns 
"""
new_fire_df = df.withColumnRenamed("Delay", "ResponseDelayedinMins")

(new_fire_df
 .select("ResponseDelayedinMins")
 .where(col("ResponseDelayedinMins") > 5)
 .show(5, False))

fire_ts_df = (new_fire_df
              .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
              .drop("CallDate")
              .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
              .drop("WatchDate")
              .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"),
                                                        "MM/dd/yyyy hh:mm:ss a"))
              .drop("AvailableDtTm"))

(fire_ts_df
 .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
 .show(5, False))

fire_ts_df_count = (fire_ts_df
                    .select("CallType")
                    .where(col("CallType").isNotNull())
                    .groupby("CallType")
                    .count()
                    .orderBy("count", ascending=False)
                    )
print("Here total count for type of fire______")
fire_ts_df_count.show(n=10, truncate=False)

"""
Other common DataFrame operations
"""
import pyspark.sql.functions  as fun
min_max_df = (fire_ts_df
              .select(fun.sum("NumAlarms"),
                      fun.avg("ResponseDelayedinMins"),
                      fun.min("ResponseDelayedinMins"),
                      fun.max("ResponseDelayedinMins")
                      )
              )
print("show min.max, avarag----------")
min_max_df.show()
min_max_df.explain(True)