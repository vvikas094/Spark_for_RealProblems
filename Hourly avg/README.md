# CloudSpark
Cloud Spark Program

**Methodology**

#####Functions

getRawDateTime -> returns the date and time of each and every tweet

getPrezDateTime -> returns the date and time when president tweeted for each of his tweet

timeConvert function -> takes in the raw date and time returned from getPrezDateTime function and returns the hour for which president tweeted

dateConvert function -> takes in the raw date and time and returns the date which has day, month and year only.

I am loading the tweets using sc.textFile function, using flatMaps to extract the hours President has tweeted and total number of days of twitter data parsed into two RDDs named hourFilter and rawDateTime. I am caching these RDDs, as I will be reusing them again.

I am returning <hour, # of tweets in that hour by president>, key-value pairs to tweetHour RDD, also returning <unique date, # of tweets on that day> - key-value pair to tweetDay RDD.

**Instructions**

Run the command

spark-submit --master yarn-client sparkProgram.py hdfs://hadoop2-0-0/twitter/*

or run the following sh file as below from the same directory where this program is present

# ./execute.sh

Note: The above sh script will run the spark program and a file named output.csv will be generated in the same folder.

I am using collect function to collect the spark program output locally and saving it in my local directory with name output.csv.

Output.csv has Total no. of days of twitter data parsed, total number of tweets made by president for each hour.

Using excel manipulations, I am calculating the average or expected number of tweets per hour by dividing the tweet count obtained by total no. of twitter data parsed.

**Note:** UTC hours are converted to Eastern Time, by reducing time by 4 hours, this calculation is done using excel. After conversion, plots are plotted, the plots are available in the document.

