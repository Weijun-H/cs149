import re
import sys
from pyspark import SparkConf, SparkContext
import time


if __name__ == '__main__':
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # CS 149 students: Implement Word Count!

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
