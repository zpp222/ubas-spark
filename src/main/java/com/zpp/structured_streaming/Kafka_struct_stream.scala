import com.zpp.structured_streaming.jdbcSink
import org.apache.spark.sql.streaming.{ProcessingTime, Trigger}
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}

object Kafka_struct_stream {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("kafka provider").getOrCreate()

    val df = spark.readStream.
      format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092")
      .option("subscribe", "stream_topic")
      .load()
    df.printSchema()

    // 输出到控制台(debug)
    //    val query = df.selectExpr("topic", "timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
    //      .writeStream
    //      .outputMode("append")
    //      .format("console")
    //      .start()
    //    query.awaitTermination()

    // 输出到外部mysql
    val url = "jdbc:mysql://master/test"
    val user = "root"
    val pwd = "123456"
    val writer: ForeachWriter[Row] = new jdbcSink(url, user, pwd); //新建自定义类
    val ds = df.selectExpr("topic", "timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream.foreach(writer)
      .outputMode("update")
      .trigger(Trigger.Continuous("10 seconds"))
      .start()
    ds.awaitTermination()

    // 输出到kafka(有bug)
    //    val ds =
    //      df.selectExpr("topic","CAST(key AS STRING)", "CAST(value AS STRING)")
    //        .writeStream
    //        .format("kafka")
    //        .option("kafka.bootstrap.servers", "master:9092,slave1:9092")
    //        .option("topic", "stream_topic")
    //        .option("checkpointLocation", "/spark")
    //        .start()
    //    ds.awaitTermination()
  }

}