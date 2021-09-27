import sys
from pyspark import SparkConf, SparkContext
import numpy as np
import time


if __name__ == '__main__':

    # Create Spark context.
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    lines = sc.textFile(sys.argv[1])

    first = time.time()

    # CS 149 students: Implement PageRank!

    print("5 highest:", highest[:5])

    last = time.time()

    print("Total program time: %.2f seconds" % (last - first))
    sc.stop()
