# encoding : utf-8
# @Time    : 3/14/19 3:12 AM
# @Author  : magic
# @Email   : 
# @File    : kafkamysql_ml.py
# @Software: PyCharm
"""
    # data如下列表样式
    #941 | 20 | M | student | 97229
    #942 | 48 | F | librarian | 78209
    #943 | 22 | M | student | 77841
#spark调用
#先要安装pip instll pymysql
#处理kafka topics txt 到MySQL库（product_txt.py作为生产者）
$SPARK_HOME/bin/spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar \
/home/hadoop/app/pm_pyspark/pyspark03/kafka_txt/kafkamysql_ml.py \
> /home/hadoop/data/kafkamysql_ml.log
"""

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import pymysql


def start():
    sconf = SparkConf()
    sconf.set('spark.cores.max', 3)
    sc = SparkContext(appName='kafkaStreamMysqltxt', conf=sconf)
    ssc = StreamingContext(sc, 5)

    brokers = "hadoop001:9092"
    topic = 'txt'
    start = 70000
    partition = 0
    user_data = KafkaUtils.createDirectStream(ssc, [topic, ], kafkaParams={"metadata.broker.list": brokers})
    # fromOffsets 设置从起始偏移量消费
    # user_data = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers},
    #                                           fromOffsets={TopicAndPartition(topic, partition): start})

    user_fields = user_data.map(lambda line: line[1].split('|'))
    gender_users = user_fields.map(lambda fields: fields[3]).map(lambda gender: (gender, 1)).reduceByKey(
        lambda a, b: a + b)
    user_data.foreachRDD(offset)  # 存储offset信息
    gender_users.pprint()
    gender_users.foreachRDD(lambda rdd: rdd.foreach(echo))  # 返回元组
    ssc.start()
    ssc.awaitTermination()


offsetRanges = []


def offset(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()


def echo(rdd):
    zhiye = rdd[0]
    num = rdd[1]
    for o in offsetRanges:
        topic = o.topic
        partition = o.partition
        fromoffset = o.fromOffset
        untiloffset = o.untilOffset
        # 结果插入MySQL
    conn = pymysql.connect(user="root", passwd="Aa123456!", host="localhost", db="test", charset="utf8")
    cursor = conn.cursor()
    sql = "insert into zhiye(zhiye,num,topic,partitions,fromoffset,untiloffset) \
    values ('%s','%d','%s','%d','%d','%d')" % (zhiye, num, topic, partition, fromoffset, untiloffset)

    cursor.execute(sql)
    conn.commit()
    conn.close()


if __name__ == '__main__':
    start()
