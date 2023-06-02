import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
import re
spark = SparkSession.builder\
                    .appName('Firstprogram')\
                    .config("spark.driver.memory", "6g") \
                    .getOrCreate()
sc=spark.sparkContext
# text_file = sc.textFile("../spark/resources/data/1gb_text.txt").repartition(50)
text_file = sc.textFile("/opt/spark/resources/data/1gb_text.txt").repartition(50)
def lower_clean_str(x):
  return re.sub(r'[^\w\s]','',x)


one_RDD = text_file.map(lower_clean_str)
one_RDD.collect()


# one_RDD.saveAsTextFile("./spark/resources/data/cleaned_1gb_text.txt")
one_RDD.saveAsTextFile("/opt/spark/resources/data/cleaned_1gb_text.txt")

spark.stop()