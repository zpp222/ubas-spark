import org.apache.spark.sql.SparkSession

object KafkaDemo {
  def main(args: Array[String]): Unit = {
    // read
    val spark: SparkSession = SparkSession.builder.appName("kafka provider").getOrCreate()
    val df = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .option("subscribe", "topic1").load()
    import spark.implicits._
    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]

    // write
    val ds = df
      .selectExpr("topic2", "CAST(key AS STRING)", "CAST(value AS STRING)")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
      .start()
  }
}