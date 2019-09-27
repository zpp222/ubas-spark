# ubas-spark
[CSDN-大数据环境搭建指导文档](https://blog.csdn.net/zphyy1988)

**ubas-spark**
>saprk-submit
```
bin/spark-submit --master yarn \
--deploy-mode client \
--num-executors 2 \
--executor-cores 2  \
--executor-memory 512M \
--class ubas1  \
task/ubas-spark.jar
```

```
bin/spark-submit --master yarn \
--deploy-mode client --num-executors 2 \
--executor-cores 2  --executor-memory 512M \
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0 \
--class Kafka_struct_stream  task/ubas-spark.jar


bin/kafka-console-producer.sh --broker-list master:9092,slave1:9092 --topic stream_topic
bin/kafka-console-consumer.sh --bootstrap-server master:9092,slave1:9092 --topic stream_topic --from-beginning
```
>
