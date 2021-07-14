from pyspark.sql import SparkSession

schema = "`Id` INT, `First` STRING, `Last` STRING, `Url` STRING,`Published` STRING, `Hits` INT, `Campaigns` STRING"
data = [[1, "Jules", "Damji", "https://tinyurl.1", "1/4/2016", 4535, ["twitter","LinkedIn"]],
[2, "Brooke","Wenig", "https://tinyurl.2", "5/5/2018", 8908, ["twitter",
"LinkedIn"]],
[3, "Denny", "Lee", "https://tinyurl.3", "6/7/2019", 7659, ["web",
"twitter", "FB", "LinkedIn"]],
[4, "Tathagata", "Das", "https://tinyurl.4", "5/12/2018", 10568,
["twitter", "FB"]],
[5, "Matei","Zaharia", "https://tinyurl.5", "5/14/2014", 40578, ["web",
"twitter", "FB", "LinkedIn"]],
[6, "Reynold", "Xin", "https://tinyurl.6", "3/2/2015", 25568,
["twitter", "LinkedIn"]]
]

spark = SparkSession.builder.appName("dataframe_schema").getOrCreate()
file_df = spark.createDataFrame(data, schema)
file_df.show()
file_df.write.format("csv")\
    .option("header",True)\
    .option('sep',',')\
    .mode("overwrite")\
    .save("/home/jaanhunzai_512/spark_projects/data/dataframe.csv")

print(file_df.printSchema())