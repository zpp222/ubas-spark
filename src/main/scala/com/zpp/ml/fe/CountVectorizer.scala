import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.SparkSession

object CountVectorizer {
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = spark.createDataFrame(Seq(
      (0, Array("a", "b", "c")),
      (1, Array("a", "b", "b", "c", "a", "d"))
    )).toDF("id", "words")

    // fit a CountVectorizerModel from the corpus
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setVocabSize(3)
      .setMinDF(2)
      .fit(df)

    cvModel.transform(df).show(false)

    //+---+------------------+-------------------------+
    //|id |words             |features                 |
    //+---+------------------+-------------------------+
    //|0  |[a, b, c]         |(3,[0,1,2],[1.0,1.0,1.0])|
    //|1  |[a, b, b, c, a, d]|(3,[0,1,2],[2.0,2.0,1.0])|
    //+---+------------------+-------------------------+
  }
}