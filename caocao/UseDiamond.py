# encoding : utf-8
# @Time    : 5/22/19 5:01 PM
# @Author  : magic
# @Email   : 
# @File    : UseDiamond.py
# @Software: PyCharm
# spark-submit --master local[2] --name ipjson /home/hadoop/app/pm_pyspark/pyspark03/caocao/UseDiamond.py hdfs://hadoop001/coolgame/caocao_s1 CCBP_Z_server2_s100_UserFlow_2019-05.json

from pyspark.sql import SparkSession
import os, sys
import pymysql
from pyspark.sql.functions import explode
import json

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()

spark = SparkSession \
    .builder \
    .appName("UseDimamond Daily") \
    .getOrCreate()

if len(sys.argv) != 3:
    print('Usage:IP <input> ', file=sys.stderr)
    sys.exit(-1)

path = sys.argv[1]
file_name = sys.argv[2]
zone_id = file_name.split('_')[3]
file_path = path + "/" + file_name
# file_path = "hdfs://hadoop001/coolgame/caocao_s1/CCBP_Z_server2_s100_UserFlow_2019-05.json"
# zone_id = 's100'

df = spark.read.json(file_path)
df.select("Date", explode("Data")).toDF("Date", "Data"). \
    select("Date", "data.Data", "data.Type").toDF("Date", "Data", "Type") \
    .createOrReplaceTempView("people")

Ips = spark.sql("select substring (Date,1,10) as Date,Data,Type from people where Type = 'UseDiamond'")
Ips_json = spark.createDataFrame(Ips.rdd.map(lambda p: (p.Date, json.loads(str(p.Data))['use'])), ["Date", "Data"]) \
    .createOrReplaceTempView("sum_user")

Ips_json_sum = spark.sql("select Date,sum(Data) as mount from sum_user group by Date").collect()

for Ip in Ips_json_sum:
    sql = "INSERT INTO DP_USE_DIMAMOND_DATA(DT_IP,ZONE_ID,AMOUNT) VALUES ('%s','%s','%s')" \
          % (Ip[0], zone_id, Ip[1])
    cursor.execute(sql)
db.commit()
db.close()
