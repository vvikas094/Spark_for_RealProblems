from __future__ import print_function
import sys,json
import time
from pyspark import SparkContext

def getRawDateTime(line):
    try:
      js = json.loads(line)
      rawDateTime = js['created_at']
      return [rawDateTime]
    except Exception as a:
      return []

def getPrezDateTime(line):
    try:
      js = json.loads(line)
      rawDateTime = js['created_at']
      if (js['user']['screen_name'] == 'PrezOno'):
        return [rawDateTime]
      else:
        return []
    except Exception as a:
      return []
      
def timeConvert(rawDateTime):
    parseTime = time.strptime(rawDateTime,'%a %b %d %H:%M:%S +0000 %Y')
    extractHour = time.strftime('%H', parseTime)
    return extractHour
    
def dateConvert(rawDateTime):
    parseTime = time.strptime(rawDateTime,'%a %b %d %H:%M:%S +0000 %Y')
    extractDate = time.strftime('%b %d %Y', parseTime)
    return extractDate

if __name__ == '__main__':
  if len(sys.argv)<2:
    print ('Enter a filename')
    sys.exit(1)

  outputcollector = None
  sc = SparkContext(appName='sparkProgram')
 
  tweets = sc.textFile(sys.argv[1])
  
  rawDateTime = tweets.flatMap(getRawDateTime)
  hourFilter = tweets.flatMap(getPrezDateTime)
  
  rawDateTime.cache()
  hourFilter.cache()
  
  tweetHour = (hourFilter.map(lambda hour: (timeConvert(hour), 1))).reduceByKey(lambda a, b: a + b)
  tweetDay =  (rawDateTime.map(lambda day: (dateConvert(day), 1))).reduceByKey(lambda a, b: a + b)
  tweetHour.cache();
  tweetDay.cache();
  
  count = tweetDay.count()
  count2 = tweetHour.count()
  
  tweetHour.foreach(print)
  outputCollector = tweetHour.collect()
  
  f = open("output.csv", "w")
  f.write("\n".join(map(lambda x: str(x), outputCollector)))
  f.write('\nNo. of Days of Twitter Data Parsed ' + str(count))
  f.close()

  print ('No. of distinct Hours tweeted by President', count2)
  print ('No. of Days of Twitter Data Parsed', count)

  #print (lengths.stats())
  
  #print (dir(tweets))
 
  #lengths.saveAsTextFile("lengths2")
 
  sc.stop()
