# encoding : utf-8
# @Time    : 3/14/19 2:50 AM
# @Author  : magic
# @Email   : 
# @File    : product_txt.py
# @Software: PyCharm

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import time


def main():
    ##生产模块
    producer = KafkaProducer(bootstrap_servers=['hadoop001:9092'])
    with open('/home/hadoop/data/ml-100k/u.user', 'r') as f:
        for line in f.readlines():
            time.sleep(3)
            producer.send("txt", line.encode())
            print(line)
            # producer.flush()


if __name__ == '__main__':
    main()