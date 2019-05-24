# encoding : utf-8
# @Time    : 3/13/19 2:28 AM
# @Author  : magic
# @Email   : 
# @File    : streamKafka.py
# @Software: PyCharm

"""演示如何使用Spark Streaming 通过Kafka Streaming实现WordCount
   执行命令：./spark-submit --master spark://XXX:7077
   --jars xxx.jar ../examples/kafka_streaming.py > log

$SPARK_HOME/bin/spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-10_2.10-2.2.3.jar \
/home/hadoop/app/pm_pyspark/pyspark03/streamKafka.py \
> /home/hadoop/data/streamKafka.log

"""

from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import sys


def start():
    sconf = SparkConf()
    sconf.set('spark.cores.max', 8)
    sc = SparkContext(appName='KafkaWordCount', conf=sconf)
    ssc = StreamingContext(sc, 2)

    numStreams = 3
    # unifiedStream = KafkaUtils.createStream(ssc,
    #                                         "hadoop001:2181",
    #                                         "streamingKafka_test_group",
    #                                         ['test'],
    #                                         kafkaParams={"metadata.broker.list": "hadoop001:9092"})
    unifiedStream = KafkaUtils.createStream(ssc, 'hadoop001:2181', 'test', {'test': 1})

    print(unifiedStream)
    # 统计生成的随机数的分布情况
    result = unifiedStream.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x + y)
    result.pprint(2)
    ssc.start()  # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate


start()
