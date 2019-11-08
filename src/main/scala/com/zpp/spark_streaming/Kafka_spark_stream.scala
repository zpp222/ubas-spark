import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Kafka_spark_stream {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("word count")
    val ssc = new StreamingContext(conf, Seconds(10))
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "master:9092,slave1:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("stream_topic")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    // 统计瞬间data信息
    stream.map(record => (record.key, record.value,record.timestamp())).print(10)
    // 时间窗口统计value个数
    stream.map(record => (record.value, 1)).reduceByKeyAndWindow((a: Int, b: Int) => (a + b), Seconds(40), Seconds(20)).print(10)

    ssc.start()
    ssc.awaitTermination()
  }

}
