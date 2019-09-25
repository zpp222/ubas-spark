import org.apache.spark.sql.SparkSession

object Kafka_struct_stream {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("kafka provider").getOrCreate()

    val df = spark.readStream.
      format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092")
      .option("subscribe", "stream_topic")
      .load()
    df.printSchema()

//    val query = df.selectExpr("topic", "timestamp", "CAST(key AS STRING)", "CAST(value AS STRING)")
//      .writeStream
//      .outputMode("append")
//      .format("console")
//      .start()
//    query.awaitTermination()

    val ds =
      df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "master:9092,slave1:9092")
        .option("checkpointLocation", "/spark")
        .option("topic", "stream_topic")
        .start()
    ds.awaitTermination()
  }

}