# encoding : utf-8
# @Time    : 3/11/19 3:13 AM
# @Author  : magic
# @Email   : 
# @File    : KafkaProducer.py
# @Software: PyCharm

import time
from kafka.producer import kafka

producer = kafka.KafkaProducer(bootstrap_servers='localhost:9092')


for _ in range(100):
    producer.send('test', b'time.time()')
    print(time.time())
