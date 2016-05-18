from __future__ import print_function
import sys, json
from pyspark import SparkContext

# Given a full tweet object, return the screen_name and (length and 1) of the tweet
def getDetails(line):
  try:
    js = json.loads(line)
    lentext = len(js['text'])
    username = js['user']['screen_name']
    return (username, (lentext, 1))
  except Exception as a:
    return 

if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("enter a filename")
    sys.exit(1)
   
  sc = SparkContext(appName="avg-t-len")
  tweets = sc.textFile(sys.argv[1],)
  
  userTlength = tweets.map(getDetails)
  
  sumUserTlength = userTlength.reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))
  topTweetedUser = sumUserTlength.top(1,key=lambda x: x[1][1])
  # top tweeter and their tweets details
  print("Top tweeting User: %s"%topTweetedUser)
  avgUserTlength = sumUserTlength.map(lambda x: (x[0], x[1][0]/x[1][1]))
  # sortByKey can be used but collect needs to be used for fetching bottom 5
  # which causes it to read all the values which takes a lot of time
  print("top: %s"%avgUserTlength.top(5, key=lambda x: x[1]))
  print("bottom: %s"%avgUserTlength.top(5, key=lambda x: -x[1]))
  sc.stop()
