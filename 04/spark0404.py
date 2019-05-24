from pyspark import SparkContext,SparkConf
import sys

if __name__ == '__main__':

    if len(sys.argv)!=2:
        print('Usage:TopN <input> ',file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    ageData = sc.textFile(sys.argv[1]).map(lambda x:x.split(' ')[1])
    totalAge = ageData.map(lambda age:int(age)).reduce(lambda a,b:a+b)
    counts = ageData.count()
    avgAge = totalAge/counts

    print(totalAge)
    print(counts)
    print(avgAge)

    sc.stop()