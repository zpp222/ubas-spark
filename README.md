# ubas-spark
[CSDN-大数据环境搭建指导文档](https://blog.csdn.net/zphyy1988)

**ubas-spark**
>saprk-submit
```
bin/spark-submit --master yarn \
--deploy-mode client --num-executors 2 \
--executor-cores 2  \
--executor-memory 512M \
--class org.ubas.spark.mongodb.demo.SparkMongoTest1  \
zpp/ubas-spark.jar
```

>
