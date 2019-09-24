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
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:2.4.4 \
--class KafkaConsumer  task/ubas-spark.jar
```
>
