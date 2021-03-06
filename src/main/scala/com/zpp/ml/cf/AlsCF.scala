import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.SparkSession

/**
 * ALS（交替最小二乘法）协同推荐
 */
object AlsCF {

  //  创建一个Rating类型，即[Int, Int, Float, Long];然后建造一个把数据中每一行转化成Rating类的函数
  case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

  val spark: SparkSession = SparkSession.builder().appName("sqlDemo").master("local[4]")
    .config("spark.scheduler.mode", "FAIR")
    .config("spark.scheduler.pool", "production")
    .getOrCreate()

  def parseRating(str: String): Rating = {
    val fields = str.split("::")
    assert(fields.size == 4)
    Rating(fields(0).toInt, fields(1).toInt, fields(2).toFloat, fields(3).toLong)
  }

  def main(args: Array[String]): Unit = {
    // 导入implicits，读取MovieLens数据集，把数据转化成Rating类型
    import spark.implicits._
    val ratings = spark.sparkContext.textFile("file:///opt/bigdata/tmp/data/sample_movielens_ratings.txt").map(parseRating).toDF()
    ratings.show()

    //把MovieLens数据集划分训练集和测试集
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))
    //使用ALS来建立推荐模型，这里我们构建了两个模型，一个是显性反馈，一个是隐性反馈
    /**
     * numBlocks 是用于并行化计算的用户和商品的分块个数 (默认为10)
     * rank 是模型中隐语义因子的个数（默认为10）
     * maxIter 是迭代的次数（默认为10）
     * regParam 是ALS的正则化参数（默认为1.0）
     * implicitPrefs 决定了是用显性反馈ALS的版本还是用适用隐性反馈数据集的版本（默认是false，即用显性反馈）
     * alpha 是一个针对于隐性反馈 ALS 版本的参数，这个参数决定了偏好行为强度的基准（默认为1.0）
     * nonnegative 决定是否对最小二乘法使用非负的限制（默认为false）
     * 可以调整这些参数，不断优化结果，使均方差变小。比如：imaxIter越大，regParam越 小，均方差会越小，推荐结果较优
     */
    val alsExplicit = new ALS().setMaxIter(5).setRegParam(0.01).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")
    val alsImplicit = new ALS().setMaxIter(5).setRegParam(0.01).setImplicitPrefs(true).setUserCol("userId").setItemCol("movieId").setRatingCol("rating")

    //把推荐模型放在训练数据上训练
    val modelExplicit = alsExplicit.fit(training)
    val modelImplicit = alsImplicit.fit(training)

    // 冷启动策略
    // "nan":  predicted value for unknown ids will be NaN.
    // "drop": rows in the input DataFrame containing unknown ids will be dropped from the output DataFrame containing predictions.
    modelExplicit.setColdStartStrategy("drop")
    modelImplicit.setColdStartStrategy("drop")

    // 使用训练好的推荐模型对测试集中的用户商品进行预测评分，得到预测评分的数据集
    val predictionsExplicit = modelExplicit.transform(test)
    val predictionsImplicit = modelImplicit.transform(test)
    //结果输出，对比一下真实结果与预测结果
    predictionsExplicit.show()
    predictionsImplicit.show()

    //通过计算模型的均方根误差来对模型进行评估，均方根误差越小，模型越准确
    val evaluator = new RegressionEvaluator().setMetricName("rmse").setLabelCol("rating").setPredictionCol("prediction")
    val rmseExplicit = evaluator.evaluate(predictionsExplicit)
    val rmseImplicit = evaluator.evaluate(predictionsImplicit)
    //打印出两个模型的均方根误差
    println(s"Explicit:Root-mean-square error = $rmseExplicit")
    println(s"Implicit:Root-mean-square error = $rmseImplicit")

    // 推荐
    // Generate top 10 movie recommendations for each user
    val userRecs = modelExplicit.recommendForAllUsers(10)
    // Generate top 10 user recommendations for each movie
    val movieRecs = modelExplicit.recommendForAllItems(10)

    // Generate top 10 movie recommendations for a specified set of users
    val users = ratings.select(alsExplicit.getUserCol).distinct().limit(3)
    val userSubsetRecs = modelExplicit.recommendForUserSubset(users, 10)
    println(s"Generate top 10 movie recommendations for a specified set of users = $userSubsetRecs")
    userSubsetRecs.show()

    // Generate top 10 user recommendations for a specified set of movies
    val movies = ratings.select(alsExplicit.getItemCol).distinct().limit(3)
    val movieSubSetRecs = modelExplicit.recommendForItemSubset(movies, 10)
    println(s"Generate top 10 user recommendations for a specified set of movies = $movieSubSetRecs")
    movieSubSetRecs.show()
    spark.stop()
  }
}
