from pyspark import SparkContext,SparkConf
import sys

if __name__ == '__main__':

    print(len(sys.argv))
    if len(sys.argv)!=3:
        print('Usage:wordcount <input> <output> ',file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    def printResult():
        counts = sc.textFile(sys.argv[1]).flatMap(lambda x:x.split(" "))\
        .map(lambda x:(x,1)).reduceByKey(lambda a,b:a+b)

        output = counts.collect()

        for (word,count) in output:
            print('%s:%i'%(word,count))

    printResult()

    def saveFile():
        sc.textFile(sys.argv[1]).flatMap(lambda x: x.split(" ")) \
            .map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)\
            .saveAsTextFile(sys.argv[2])

    saveFile()
    sc.stop()