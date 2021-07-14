from pyspark import SparkContext, SparkConf

sc = SparkContext(conf=SparkConf().setAppName("demo spark").setMaster("local"))
txt = sc.textFile("/home/jaanhunzai_512/spark_projects/data/text.txt")

spark_words = txt.filter(lambda line: "spark" in line.lower())


with open ("/home/jaanhunzai_512/spark_projects/data/result.txt", "w") as f:
    f.write(f'number of LINES: {txt.count()}\n')
    f.write(f'number of spark words: {spark_words.count()}\n')

a = sc.parallelize([1,2,3,4,5])
print(a.collect())

