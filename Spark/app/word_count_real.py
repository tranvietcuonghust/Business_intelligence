import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import re
import pandas as pd
spark = SparkSession.builder\
                    .appName('word_count_program')\
                    .config("spark.driver.memory", "6g") \
                    .getOrCreate()
sc=spark.sparkContext
# text_file = sc.textFile("../spark/resources/data/1gb_text.txt").repartition(50)
# text_file = sc.textFile("/opt/spark/resources/data/1gb_text.txt").repartition(30)
text_file = sc.textFile("hdfs://namenodetvc:9010/user/root/data/word_count/1gb_text.txt").repartition(30)
counts = text_file.flatMap(lambda line: line.split(" ")) \
                            .map(lambda word: (word, 1)) \
                           .reduceByKey(lambda x, y: x + y)

# output = counts.collect()
# one_RDD = text_file.map(lower_clean_str)
# one_RDD.collect()


# one_RDD.saveAsTextFile("./spark/resources/data/cleaned_1gb_text.txt")
counts.saveAsTextFile("/opt/spark/resources/data/word_count.txt")
# df=counts.toDF()
# # dfp= df.toPandas()
# # df.coalesce(1).write.format("text").option("header", "false").mode("append").save("/opt/spark/resources/data/word_count.txt")
# counts.saveAsTextFile
spark.stop()