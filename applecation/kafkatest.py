# encoding : utf-8
# @Time    : 3/13/19 2:28 AM
# @Author  : magic
# @Email   :
# @File    : kafkatest.py
# @Software: PyCharm
"""演示如何使用Spark Streaming 通过Kafka Streaming实现WordCount
   执行命令：./spark-submit --master spark://XXX:7077
   --jars xxx.jar ../examples/kafka_streaming.py > log

$SPARK_HOME/bin/spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-10_2.10-2.2.3.jar \
/home/hadoop/app/pm_pyspark/pyspark03/kafkatest.py \
> /home/hadoop/data/kafkatest.log

   Kafka数据源程序：
"""
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

offsets = []


def out_put(m):
    print(m)


def store_offset(rdd):
    global offsets
    offsets = rdd.offsetRanges()
    return rdd


def print_offset(rdd):
    for o in offsets:
        print("%s- %s -%s -%s -%s" % (o.topic, o.partition, o.fromOffset, o.untilOffset, o.untilOffset - o.fromOffset))


config = SparkConf()
scontext = SparkContext(appName='kafka_pyspark_test', )
stream_context = StreamingContext(scontext, 2)
msg_stream = KafkaUtils.createDirectStream(stream_context, ['test', ],
                                           kafkaParams={"metadata.broker.list": "hadoop001:9092,"})
result = msg_stream.map(lambda x: json.loads(x).keys()).reduce(out_put)
msg_stream.transform(store_offset, ).foreachRDD(print_offset)
result.pprint()
stream_context.start()
