# encoding : utf-8
# @Time    : 5/21/19 4:09 PM
# @Author  : magic
# @Email   : 
# @File    : AccountNumber.py
# @Software: PyCharm
# tongjiyonghuliang
# spark-submit --master local[2] --name ipjson /home/hadoop/app/pm_pyspark/pyspark03/caocao/AccountNumber.py hdfs://hadoop001/coolgame/caocao_s1 CCBP_Z_server2_s100_UserFlow_2019-05.json
from pyspark.sql import SparkSession
import os, sys
import pymysql

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()

spark = SparkSession \
    .builder \
    .appName("Account Number Daily") \
    .getOrCreate()

if len(sys.argv) != 3:
    print('Usage:IP <input> ', file=sys.stderr)
    sys.exit(-1)

path = sys.argv[1]
file_name = sys.argv[2]
zone_id = file_name.split('_')[3]
file_path = path + "/" + file_name
# file_path ="hdfs://hadoop001/coolgame/caocao_s1/CCBP_Z_server2_s100_UserFlow_2019-05.json"
# zone_id = 's100'

df = spark.read.json(file_path)

df.createOrReplaceTempView("people")
LastVisitIp = spark.sql(
    "SELECT distinct substring (Date,1,10) as Date,UserData.Uid as Uid \
    FROM people ")

LastVisitIp.createOrReplaceTempView("account")
LastVisit = spark.sql(
    "select Date,count(Uid) as count_c from account  group by Date")

Ips = LastVisit.rdd.map(lambda p: (p.Date, p.count_c)).collect()
for Ip in Ips:
    date_id = Ip[0]
    count = Ip[1]

    sql = "INSERT INTO DP_ACCOUNT_DATA(DT_IP,ZONE_ID,COUNT) VALUES ('%s','%s','%s')" \
          % (date_id, zone_id, count)
    cursor.execute(sql)

db.commit()
db.close()
