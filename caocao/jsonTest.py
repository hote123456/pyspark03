# encoding : utf-8
# @Time    : 5/23/19 10:02 AM
# @Author  : magic
# @Email   : 
# @File    : jsonTest.py
# @Software: PyCharm

from pyspark.sql import SparkSession
import os, sys
import pymysql
import json
from pyspark.sql.functions import explode

spark = SparkSession \
    .builder \
    .appName("Account Number Daily") \
    .getOrCreate()

file_path = "hdfs://hadoop001/coolgame/caocao_s1/CCBP_Z_server2_s100_UserFlow_2019-05.json"
df = spark.read.json(file_path)

df.createTempView("dftable")
dftable = spark.sql("select Date,explode(Data) as data from dftable where date='2019-05-01 00:00:52'").collect()
for dt1 in dftable:
    print(len(dt1), dt1[0], dt1[1])
dftable2 = spark.sql("select Date,Data from dftable where date='2019-05-01 00:00:52'").collect()
for dt1 in dftable2:
    print(len(dt1), dt1[0], dt1[1])

# a = df.select("Date", explode("Data")).toDF("date","data")
# b= a.select("Date", "data.Data","data.Type")
# a.show()
# a.printSchema()
# b.show()
# b.printSchema()
