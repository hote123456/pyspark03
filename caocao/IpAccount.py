# encoding : utf-8
# @Time    : 5/20/19 11:23 AM
# @Author  : magic
# @Email   : 
# @File    : IpAccount.py
# @Software: PyCharm

# get json data
#spark-submit --master local[2] --name ipjson /home/hadoop/app/pm_pyspark/pyspark03/caocao/IpAccount.py hdfs://hadoop001/coolgame/caocao_s1 CCBP_Z_server2_s100_UserFlow_2019-05.json
from pyspark.sql import SparkSession
import os, sys
import pymysql

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()

spark = SparkSession \
    .builder \
    .appName("Json Data") \
    .getOrCreate()

if len(sys.argv) != 3:
    print('Usage:IP <input> ', file=sys.stderr)
    sys.exit(-1)

path = sys.argv[1]
file_name = sys.argv[2]
zone_id = file_name.split('_')[3]
# file_path = "hdfs://hadoop001/coolgame/caocao_s1/CCBP_Z_server2_s100_UserFlow_2019-05.json"
# zone_id = 's100'

df = spark.read.json(path+"/"+file_name)

df.createOrReplaceTempView("people")

LastVisitIp = spark.sql(
    "SELECT distinct substring (Date,1,10) as Date,UserData.LastVisitIp as ips,UserData.Uid as account \
    FROM people WHERE UserData.LastVisitIp is not null ")

Ips = LastVisitIp.rdd.map(lambda p: (p.Date, p.ips, p.account)).collect()
for Ip in Ips:
    date_id = Ip[0]
    ip_id = Ip[1]
    account = Ip[2]

    sql = "INSERT INTO DP_IP_DATA(DT_IP,ZONE_ID,IP,ACCOUNT) VALUES ('%s','%s','%s','%s')" \
          % (date_id, zone_id, ip_id, account)
    cursor.execute(sql)

db.commit()
db.close()


