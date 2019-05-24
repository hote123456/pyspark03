from pyspark import SparkConf,SparkContext

#create SparlConf
conf = SparkConf().setMaster('local[2]').setAppName('spark0301')

sc = SparkContext(conf=conf)

#yewuluoji

date = [1,2,3,4,5]

sparkdate = sc.parallelize(date)

print(sparkdate.collect())

sc.stop()