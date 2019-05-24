# encoding : utf-8
# @Time    : 3/18/19 1:32 AM
# @Author  : magic
# @Email   : 
# @File    : network_wordcount.py
# @Software: PyCharm

"""
count word from network
nc -lk 9999
spark-submit /home/hadoop/app/pm_pyspark/pyspark03/applecation/network_wordcount.py localhost 9999

"""

from __future__ import print_function

import sys

from pyspark import SparkContext,SparkConf
from pyspark.streaming import StreamingContext

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage:network need <hostname> <port>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName='networkPython')
    ssc = StreamingContext(sc, 5)

    lines = ssc.socketTextStream(sys.argv[1], int(sys.argv[2]))
    counts = lines.flatMap(lambda x: x.split(' ')) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b)
    counts.pprint()

    ssc.start()
    ssc.awaitTermination()
