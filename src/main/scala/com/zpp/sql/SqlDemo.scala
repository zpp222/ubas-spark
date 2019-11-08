import org.apache.spark.sql.SparkSession

object SqlDemo{
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("sqlDemo")
      .config("spark.scheduler.mode","FAIR")
      .config("spark.scheduler.pool","production")
      .getOrCreate()
  }
}
