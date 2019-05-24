# encoding : utf-8
# @Time    : 5/22/19 4:01 PM
# @Author  : magic
# @Email   : 
# @File    : VipAccount.py
# @Software: PyCharm
# spark-submit --master local[2] --name ipjson /home/hadoop/app/pm_pyspark/pyspark03/caocao/VipAccount.py hdfs://hadoop001/coolgame/caocao_s1 CCBP_Z_server2_s100_UserFlow_2019-05.json

from pyspark.sql import SparkSession
import os, sys
import pymysql

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()

spark = SparkSession \
    .builder \
    .appName("VipAccount Daily") \
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

df.createOrReplaceTempView("people")
LastVisitIp = spark.sql(
    "SELECT distinct substring (Date,1,10) as Date,UserData.Uid as Uid,UserData.VipLv as VipLv \
    FROM people ")

Ips = LastVisitIp.rdd.map(lambda p: p).collect()
for Ip in Ips:
    date_id = Ip[0]
    Uid = Ip[1]
    VipLv = Ip[2]

    sql = "INSERT INTO DP_VIP_ACCOUNT_DATA(DT_IP,ZONE_ID,ACCOUNT,VIP_LV) VALUES ('%s','%s','%s','%s')" \
          % (date_id, zone_id, Uid, VipLv)
    cursor.execute(sql)

db.commit()
db.close()
