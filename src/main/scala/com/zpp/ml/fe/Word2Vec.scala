import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Row, SparkSession}

/**
 * Skip-gram模型
 */
object Word2Vec {
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    // Input data: Each row is a bag of words from a sentence or document.
    val documentDF = spark.createDataFrame(Seq(
      "Hi I heard about Spark".split(" "),
      "I wish Java could use case classes".split(" "),
      "Logistic regression models are neat".split(" "),
      "I don't like xian".split(" ")
    ).map(Tuple1.apply)).toDF("text")

    // Learn a mapping from words to Vectors.
    val word2Vec = new Word2Vec()
      .setInputCol("text")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(0)
    val model = word2Vec.fit(documentDF)

    val result = model.transform(documentDF)
    result.collect().foreach { case Row(text: Seq[_], features: Vector) =>
      println(s"Text: [${text.mkString(", ")}] => \nVector: $features\n") }

    // 相似性
    model.findSynonyms("Spark", 20).collect().foreach(println)
  }
}