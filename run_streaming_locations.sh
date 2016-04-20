#!/bin/bash

spark-submit --class es.dmr.doyle.spark.locations.DoyleLocationsJob --master yarn-cluster --num-executors 2 --driver-memory 850M --executor-memory 850M --executor-cores 1 --queue default target/scala-2.10/spark-streaming-doyle-*.jar localhost:2181 spark-consumer doyle-episodes 1 spark-locations-occurences