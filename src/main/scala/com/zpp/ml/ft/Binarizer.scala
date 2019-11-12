import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.Binarizer
/**
 * 二值化是将数值特征阈值化为二值(0/1)特征的过程
 * 二值化器采用常用的参数inputCol和outputCol，以及二值化的阈值。
 * 将大于阈值的特征值二值化到1.0;等于或小于阈值的值被二进制化为0.0。
 * inputCol同时支持Vector和Double类型
 */
object Binarizer{
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show(false)
  }
}