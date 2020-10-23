#!/bin/sh


set -o errexit
set -o nounset


echo "wait"


start-history-server.sh
echo "History server is sarting ...."
sleep 2
echo "Master is sarting ...."
start-master.sh

sleep 1
echo "Master started at port 8080 ...."

echo "worker is sarting ...."

start-slave.sh spark://spark:7077

echo "worker started at port 8081 ...."

echo "producing data to kafka: topic locations"
python app/produce_data.py

sleep 1
echo "producing data to kafka: Done!"

sleep 2
echo "Running on spark://spark:7077"

spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.7,org.apache.kafka:kafka-clients:2.3.0 --jars app/jars/postgresql-9.4.1207.jre6.jar app/stream_process.py

sleep infinity