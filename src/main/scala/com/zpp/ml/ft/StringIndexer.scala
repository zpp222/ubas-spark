import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.StringIndexer

/**
 * StringIndexer可以把字符串的列按照出现频率进行排序，出现次数最高的对应的Index为0
 *
 */
object StringIndexer {
  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val df = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("keep") // error

    val df1 = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"), (6, "d"))
    ).toDF("id", "category")

    val indexed = indexer.fit(df).transform(df1)
    indexed.show(false)
  }
}