import java.sql.Date

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

// nc -lk 9999
// Socket source (for testing)
// 不支持容错

object SocketConsumer {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder.appName("socket consumer").getOrCreate()
    import spark.implicits._
    val lines: DataFrame = spark.readStream.format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    lines.printSchema()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
