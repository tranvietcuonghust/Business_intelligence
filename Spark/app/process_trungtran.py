from pyspark.sql.functions import col, when
from pyspark.sql.functions import trim, regexp_replace

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
import numpy as np
import re
import pandas as pd
import datetime

spark = SparkSession.builder.appName('clean_data_trungtran').getOrCreate()
sqlContext = SQLContext(spark)

schema = StructType([ \
    StructField("Battery", StringType(), True), \
    StructField("Brand", StringType(), True), \
    StructField("CPU", StringType(), True), \
    StructField("Color", StringType(), True), \
    StructField("Display", StringType(), True), \
    StructField("Graphics", StringType(), True), \
    StructField("MFG_year", StringType(), True), \
    StructField("Name", StringType(), True), \
    StructField("OS", StringType(), True), \
    StructField("Price", StringType(), True), \
    StructField("Ram", StringType(), True), \
    StructField("Size", StringType(), True), \
    StructField("Storage", StringType(), True), \
    StructField("URL", StringType(), True), \
    StructField("Weight", StringType(), True), \
    StructField("Wireless", StringType(), True) \
  ])

df = spark.read.option("delimiter", ",") \
      .option("escape", "\"") \
      .option("multiLine","true") \
      .format("csv") \
      .option("header", True) \
      .schema(schema) \
      .load("/opt/spark/resources/data/laptop88.csv")
      # .load("../spark/resources/data/laptop_trungtran.csv")
# # for col in df.columns:
# df = df.withColumn("Battery", regexp_replace(col('Battery'), "^\n", "")).withColumn("Battery", trim(col('Battery')))
# for column in ["Battery","Brand","CPU","Color","Display","Graphics","MFG_year","Name","OS","Price","Ram","Size","Storage","URL","Weight","Wireless"]:
# df = df.filter(~(col("Price").like("%Liên hệ%")))
# df = df.filter(df["Price"] != "Liên hệ")
df = df.filter(col("Price").isNotNull() & ~(col("Price").contains("Liên hệ")))
# for column in df.columns:
#     df = df.withColumn(column, when(df[column].isNull(), df[column]).otherwise(regexp_replace(df[column], "^\n", ""))) 
#     df=df.withColumn(column, when(df[column].isNull(), df[column]).otherwise(trim(df[column])))
df.show()
df1pd = df.toPandas()
# df1.write.format("csv").mode('append').options(header='True', delimiter=',',escape ='\"') \
#  .save('../spark/resources/data/cleaned_house_price_data.csv')
df1pd.to_csv("/opt/spark/resources/data/cleaned_laptop_laptop88.csv", index=False, header=True)

spark.stop()

