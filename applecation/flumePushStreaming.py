# encoding : utf-8
# @Time    : 3/19/19 2:05 AM
# @Author  : magic
# @Email   : 
# @File    : flumePushStreaming.py
# @Software: PyCharm

"""
count word from flume
flume push message to sparkStreaming
1.spark-submit --master spark://hadoop001:7077 \
--jars $SPARK_HOME/jars/spark-streaming-flume-assembly_2.11-2.3.0.jar \
/home/hadoop/app/pm_pyspark/pyspark03/applecation/flumePushStreaming.py hadoop001 41414 \
> /home/hadoop/data/flumePushStreaming.log

2.flume-ng agent --name a1 --conf $FLUME_HOME/conf --conf-file $FLUME_HOME/conf/flume_push_streaming.conf -Dflume.root.logger=INFO,console

3.telnet hadoop001 44444

"""

from __future__ import print_function

import sys

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.flume import FlumeUtils

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage:network need <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName='flumePushStreaming')
    ssc = StreamingContext(sc, 5)
    hostname, port = sys.argv[1:]

    print(hostname + '----------' + port)

    kvs = FlumeUtils.createStream(ssc, hostname, int(port))
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda x: x.split(' ')).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
