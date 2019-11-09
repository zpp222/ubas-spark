import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SparkSession

object TFIDF {
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    // 句子
    val sentenceData = spark.createDataFrame(Seq(
      (0.0, "Hi I heard about Spark"),
      (0.0, "I wish Java could use case classes"),
      (1.0, "Logistic regression models are neat")
    )).toDF("label", "sentence")

    val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words")
    // 将原始文本分割成单词
    val wordsData = tokenizer.transform(sentenceData)
    wordsData.select("*").show(false)

    // 单词列转化为特征向量(词频)
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)
    val featurizedData = hashingTF.transform(wordsData)
    featurizedData.select("*") .show(false)

    // 词频特征向量进行修正，使其更能体现不同词汇对文本的区别能力
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    // 每一个单词对应的TF-IDF度量值
    val rescaledData = idfModel.transform(featurizedData)
    rescaledData.select("label", "features").show(false)

    spark.stop()
  }

}