# To run, do: spark-submit --master yarn-client ogramoq.py hdfs://hadoop2-0-0/data/1gram/googlebooks-eng-all-1gram-20120701-0

from __future__ import print_function
import sys
from pyspark import SparkContext

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt

def plot(words):
  fig=plt.figure()
  values=map(lambda x: x[1], words)
  labels=map(lambda x: x[0], words)
  plt.bar(range(len(labels)),values)
  plt.xticks(range(len(values)),labels)
  plt.xlabel("Year")
  plt.ylabel("Avg word length")
  plt.title("Word Length Plot")
  fig.savefig('barchar.pdf')
  plt.close()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="meApp")
  
  onegrams = sc.textFile(sys.argv[1],)
  texts=onegrams.map(lambda line:line.split())
  output=texts.map(lambda x:(x[1],(len(x[0]),1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
  avgO=output.map(lambda x: (x[0],float(x[1][0])/x[1][1])).sortByKey(True,100)
  avgO.saveAsTextFile("wordLength")
  plot(avgO.collect())
  sc.stop
