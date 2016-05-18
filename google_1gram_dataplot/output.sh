echo 'Uses matplotlib library to display plots - Please Install it'
echo `hadoop fs -rm -r -f fullOutput`
echo `spark-submit --master yarn-client ogramoq.py hdfs://hadoop2-0-0/data/1gram/`
echo `hadoop fs -get fullOutput`
