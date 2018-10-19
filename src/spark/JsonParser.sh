FOLDER=$PWD
cd /usr/local/spark/bin/

spark-submit --master spark://ip-10-0-0-4:7077 \
--driver-memory 4G \
--executor-memory 4G \
$FOLDER/JsonParser.py