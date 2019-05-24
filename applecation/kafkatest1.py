# encoding : utf-8
# @Time    : 3/14/19 1:51 AM
# @Author  : magic
# @Email   : 
# @File    : kafkatest1.py
# @Software: PyCharm
"""
$SPARK_HOME/bin/spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-10_2.10-2.2.3.jar \
/home/hadoop/app/pm_pyspark/pyspark03/streamKafka.py \
> /home/hadoop/data/streamKafka.log

"""

import time
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from operator import add

sc = SparkContext(master="local[1]", appName="PythonSparkStreamingRokidDtSnCount")
ssc = StreamingContext(sc, 2)
zkQuorum = 'hadoop001:2181'
topic = {'test': 1}
groupid = "test-consumer-group"
lines = KafkaUtils.createStream(ssc, zkQuorum, groupid, topic)
lines1 = lines.flatMap(lambda x: x.split("\n"))
valuestr = lines1.map(lambda x: x.value.decode())
valuedict = valuestr.map(lambda x: eval(x))
message = valuedict.map(lambda x: x["message"])
rdd2 = message.map(lambda x: (
    time.strftime("%Y-%m-%d", time.localtime(float(x.split("\u0001")[0].split("\u0002")[1]) / 1000)) + "|" +
    x.split("\u0001")[1].split("\u0002")[1], 1)).map(lambda x: (x[0], x[1]))
rdd3 = rdd2.reduceByKey(add)
rdd3.saveAsTextFiles("/home/hadoop/data/wordcount/count")
rdd3.pprint()
ssc.start()
ssc.awaitTermination()
