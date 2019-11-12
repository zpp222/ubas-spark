import org.apache.spark.ml.feature.NGram
import org.apache.spark.sql.SparkSession

/**
 * 把单词转成一个个连续词输出
 */
object NGram{
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val wordDataFrame = spark.createDataFrame(Seq(
      (0, Array("Hi", "I", "heard", "about", "Spark")),
      (1, Array("I", "wish", "Java", "could", "use", "case", "classes")),
      (2, Array("Logistic", "regression", "models", "are", "neat"))
    )).toDF("id", "words")

    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")

    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)
    //+------------------------------------------------------------------+
    //|ngrams                                                            |
    //+------------------------------------------------------------------+
    //|[Hi I, I heard, heard about, about Spark]                         |
    //|[I wish, wish Java, Java could, could use, use case, case classes]|
    //|[Logistic regression, regression models, models are, are neat]    |
    //+------------------------------------------------------------------+
  }
}