# encoding : utf-8
# @Time    : 5/21/19 6:10 PM
# @Author  : magic
# @Email   : 
# @File    : RechangeDaily.py
# @Software: PyCharm
# 每日充值账号金额  --SettleOrder 注意币种(目前可以忽略) SettleOrder   结算所有订单

# spark-submit --master local[2] --name ipjson /home/hadoop/app/pm_pyspark/pyspark03/caocao/RechangeDaily.py hdfs://hadoop001/coolgame/caocao_s1 CCBP_Z_server2_s100_UserFlow_2019-05.json
from pyspark.sql import SparkSession
import os, sys, json
import pymysql

db = pymysql.connect("hadoop001", "root", "Aa123456!", "caocao")
cursor = db.cursor()

spark = SparkSession \
    .builder \
    .appName("Recharge Daily") \
    .getOrCreate()

if len(sys.argv) != 3:
    print('Usage:IP <input> ', file=sys.stderr)
    sys.exit(-1)

path = sys.argv[1]
file_name = sys.argv[2]
zone_id = file_name.split('_')[3]
file_path = path + "/" + file_name

# test config
# file_path = "hdfs://hadoop001/coolgame/caocao_s1/CCBP_Z_server2_s100_UserFlow_2019-05.json"
# zone_id = 's100'

df = spark.read.json(file_path)

df.createOrReplaceTempView("people")
LastVisitIp = spark.sql(
    "SELECT substring (Date,1,10) as Date,Data,UserData.Uid as Uid,UserData.Platform as Platform\
    FROM people where Op='SettleOrder'")

# LastVisitIp.printSchema()
LastVisitIp.createOrReplaceTempView("SettleOrder")
SettleOrder = spark.sql(
    "SELECT Date,Data.Data as data ,Data.Type as type,Uid,Platform\
    FROM SettleOrder ")

IpOs = SettleOrder.rdd.map(lambda p: (p.Date, p.data, p.Uid, p.Platform)).collect()

for Ipo in IpOs:
    totel_amount = 0
    date_id = Ipo[0]
    account = Ipo[2]
    platform = Ipo[3]

    # ('2019-05-03', ['{"old":859,"new":919,"give":60}', '{"OrderId":"201905031109417890","Channel":"ccbp2_coolgamesdk","Amount":"6.00"}'])
    for x in Ipo[1]:
        x_json = json.loads(x)
        t_Amount = x_json.get('Amount')
        if t_Amount == None:
            t_Amount = 0
        totel_amount = totel_amount + float(t_Amount)

    # print(Ipo[0] + '-------------' + str(totel_amount))
    sql = "INSERT INTO DP_AMOUNT_DATA(DT_IP,ZONE_ID,AMOUNT,ACCOUNT,PLATFORM) VALUES ('%s','%s','%s','%s','%s')" \
          % (date_id, zone_id, totel_amount, account, totel_amount)
    cursor.execute(sql)

db.commit()
db.close()
