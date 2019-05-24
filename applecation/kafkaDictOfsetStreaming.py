# encoding : utf-8
# @Time    : 3/20/19 2:48 AM
# @Author  : magic
# @Email   : 
# @File    : kafkaDictOfsetStreaming.py
# @Software: PyCharm
"""
count word from flume
sparkStreaming dict message from kafka

1.启动kafka服务
kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
2.创建wordcount topic
kafka-topics.sh --create --zookeeper hadoop001:2181 --replication-factor 1 --partitions 1 --topic wordcount
3.kafka product
sh kafka-console-producer.sh --broker-list hadoop001:9092 --topic wordcount
4.kafka consumer
kafka-console-consumer.sh --bootstrap-server hadoop001:9092 --topic wordcount --from-beginning
5.spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar \
/home/hadoop/app/pm_pyspark/pyspark03/applecation/kafkaDictOfsetStreaming.py hadoop001:9092 wordcount \
> /home/hadoop/data/kafkaDictOfsetStreaming.log

"""
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import os, sys
import json

broker_list, topic_name = sys.argv[1:]
timer = 5
offsetRanges = []


def store_offset_ranges(rdd):
    global offsetRanges
    offsetRanges = rdd.offsetRanges()
    return rdd


def save_offset_ranges(rdd):
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    data = dict()
    f = open(record_path, "w")
    # print("offset.txt----record_path%s" % (record_path))
    for o in offsetRanges:
        data = {"topic": o.topic, "partition": o.partition, "fromOffset": o.fromOffset, "untilOffset": o.untilOffset}
        print("%s - %s - %s - %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset))
    f.write(json.dumps(data))
    f.close()


def deal_data(rdd):
    data = rdd.collect()
    for d in data:
        # do something
        pass


def save_by_spark_streaming():
    root_path = os.path.dirname(os.path.realpath(__file__))
    record_path = os.path.join(root_path, "offset.txt")
    print("offset.txt--save--record_path%s" % (record_path))
    from_offsets = {}
    # 获取已有的offset，没有记录文件时则用默认值即最大值
    if os.path.exists(record_path):
        f = open(record_path, "r")
        offset_data = json.loads(f.read())
        f.close()
        if offset_data["topic"] != topic_name:
            raise Exception("the topic name in offset.txt is incorrect")

        topic_partion = TopicAndPartition(offset_data["topic"], offset_data["partition"])
        from_offsets = {topic_partion: int(offset_data["untilOffset"])}  # 注意设置起始offset时的方法
        print("start from offsets: %s" % (from_offsets))

    sc = SparkContext(appName="Realtime-Analytics-Engine")
    ssc = StreamingContext(sc, int(timer))

    kvs = KafkaUtils.createDirectStream(ssc=ssc, topics=[topic_name], fromOffsets=from_offsets,
                                        kafkaParams={"metadata.broker.list": broker_list})

    # 官网offset说明
    # directKafkaStream \
    #     .transform(storeOffsetRanges) \
    #     .foreachRDD(printOffsetRanges)

    # 事务处理
    # kvs.foreachRDD(lambda rec: deal_data(rec))

    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()
    # 存储offset
    kvs.transform(store_offset_ranges).foreachRDD(save_offset_ranges)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()


if __name__ == '__main__':
    save_by_spark_streaming()
