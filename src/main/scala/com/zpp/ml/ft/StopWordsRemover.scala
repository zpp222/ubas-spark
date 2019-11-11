import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StopWordsRemover

object StopWordsRemover{
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered")

    val dataSet = spark.createDataFrame(Seq(
      (0, Seq("I", "saw", "the", "red", "balloon")),
      (1, Seq("Mary", "had", "a", "little", "lamb"))
    )).toDF("id", "raw")

    remover.transform(dataSet).show(false)

    //+---+----------------------------+--------------------+
    //|id |raw                         |filtered            |
    //+---+----------------------------+--------------------+
    //|0  |[I, saw, the, red, balloon] |[saw, red, balloon] |
    //|1  |[Mary, had, a, little, lamb]|[Mary, little, lamb]|
    //+---+----------------------------+--------------------+
  }
}