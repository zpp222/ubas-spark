import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.{VectorAssembler, VectorSizeHint}
import org.apache.spark.ml.linalg.Vectors
object VectorSizeHint{
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val dataset = spark.createDataFrame(
      Seq(
        (0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0),
        (0, 18, 1.0, Vectors.dense(0.0, 10.0), 0.0))
    ).toDF("id", "hour", "mobile", "userFeatures", "clicked")

    val sizeHint = new VectorSizeHint()
      .setInputCol("userFeatures")
      .setHandleInvalid("skip")
      .setSize(3)

    val datasetWithSize = sizeHint.transform(dataset)
    println("Rows where 'userFeatures' is not the right size are filtered out")
    datasetWithSize.show(false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("hour", "mobile", "userFeatures"))
      .setOutputCol("features")

    // This dataframe can be used by downstream transformers as before
    val output = assembler.transform(datasetWithSize)
    println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
    output.select("features", "clicked").show(false)
  }
}