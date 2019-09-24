import org.apache.spark.sql.SparkSession

object KafkaProvider {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("kafka provider").getOrCreate()

    val df = spark.readStream.
      format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // write
    val ds = df
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .outputMode("append")
      .format("kafka")
      .option("kafka.bootstrap.servers", "master:9092,slave1:9092")
      .option("checkpointLocation","/spark")
      .option("topic", "stream_topic")
      .start()

    ds.awaitTermination()
  }
}