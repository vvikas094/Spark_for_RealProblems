# To run, do: spark-submit --master yarn-client ogramoq.py hdfs://hadoop2-0-0/data/1gram/googlebooks-eng-all-1gram-20120701-0

from __future__ import print_function
import sys
from pyspark import SparkContext

import matplotlib
matplotlib.use('Agg')

import matplotlib.pyplot as plt
# Words Plot figure to obtain the Number of words per year
def plot(words):
  fig=plt.figure()
  values=map(lambda x: x[1], words)
  labels=map(lambda x: x[0], words)
  plt.bar(range(len(labels)),values)
  plt.xticks(range(len(values)),labels)
  plt.xlabel("Year")
  plt.ylabel("Number of words")
  plt.title("Words Plot")
  fig.savefig('./barchar.svg', format="svg")
  plt.close()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="meApp")
  
  onegrams = sc.textFile(sys.argv[1],)
  
  texts = onegrams.map(lambda line: line.split())
  output = texts.map(lambda l: (l[1],1)).reduceByKey(lambda a,b: a+b)
  
  sortOutput = output.map(lambda x: (x[0], x[1])).sortByKey(True)

  sortOutput.saveAsTextFile("fullOutput")
  # All the output is plotted in the figure and the plot is saved
  plot(sortOutput.collect())
  sc.stop()
