from pyspark import  SparkConf,SparkContext

if __name__ == '__main__':
    conf = SparkConf().setMaster('local[2]').setAppName('spark0401')

    data = [1,2,3,4,5]
    anima = ['dog','tiger','lion','cat','panther','eagle']
    sc = SparkContext(conf=conf)

    def my_map(data):
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x:x*2)
        print(rdd2.collect())

    def my_map2(data):
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x: (x,1))
        print(rdd2.collect())

    my_map(data)
    my_map2(anima)
    sc.stop()