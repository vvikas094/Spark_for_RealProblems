echo 'Uses matplotlib library to display plots - Please Install it'
echo `hadoop fs -rm -r -f wordLength1`
echo `spark-submit --master yarn-client ogram3q.py hdfs://hadoop2-0-0/data/1gram`
echo `hadoop fs -get wordLength1`
