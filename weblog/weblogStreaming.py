# encoding : utf-8
# @Time    : 3/21/19 2:37 AM
# @Author  : magic
# @Email   : 
# @File    : weblogStreaming.py
# @Software: PyCharm
"""
data from weblog-->flume-->kafka-->sparkstreaming

spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-kafka-0-8-assembly_2.11-2.3.0.jar \
/home/hadoop/app/pm_pyspark/pyspark03/weblog/weblogStreaming.py hadoop001:9092 txt \
> /home/hadoop/data/weblogStreaming.log
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import sys

brokers, topic = sys.argv[1:]

sc = SparkContext(appName='weblogStreaming')
ssc = StreamingContext(sc, 3)

kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
lines = kvs.map(lambda x: x[1])
kvs.pprint()
lines.pprint()

"""
接收到的kafa的数据如下：
(None, '143.156.98.168\t2019-03-21 20:57:32\t"GET /class/12.html HTTP/1.1" \t404\t-')
(None, '46.72.167.63\t2019-03-21 20:57:32\t"GET /class/18.html HTTP/1.1" \t404\t-')
(None, '132.124.87.156\t2019-03-21 20:57:32\t"GET /class/18.html HTTP/1.1" \t404\t-')
(None, '156.167.30.143\t2019-03-21 20:57:32\t"GET /class/18.html HTTP/1.1" \t500\thttp://www.sogou.com/web?query=大数据面试')
(None, '29.168.156.63\t2019-03-21 20:57:32\t"GET /class/78.html HTTP/1.1" \t500\thttp://www.sogou.com/web?query=Hadoop 基础')
(None, '124.30.167.87\t2019-03-21 20:57:32\t"GET /class/130.html HTTP/1.1" \t404\t-')
(None, '124.55.30.72\t2019-03-21 20:57:32\t"GET /class/145.html HTTP/1.1" \t200\thttp://www.sogou.com/web?query=Spark SQL 实战')
(None, '72.87.167.29\t2019-03-21 20:57:32\t"GET /class/18.html HTTP/1.1" \t500\t-')
(None, '156.124.63.46\t2019-03-21 20:57:32\t"GET /class/146.html HTTP/1.1" \t500\t-')
(None, '72.156.187.55\t2019-03-21 20:57:32\t"GET /class/18.html HTTP/1.1" \t500\t-')
"""

ssc.start()
ssc.awaitTermination()
