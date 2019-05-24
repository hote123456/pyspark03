# encoding : utf-8
# @Time    : 3/19/19 11:15 PM
# @Author  : magic
# @Email   : 
# @File    : kafkaDictStreaming.py
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
/home/hadoop/app/pm_pyspark/pyspark03/applecation/kafkaDictStreaming.py hadoop001:9092 wordcount \
> /home/hadoop/data/kafkaDictStreaming.log

"""

from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: direct_kafka_wordcount.py <broker_list> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line.split(" "))\
        .map(lambda word: (word, 1))\
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
