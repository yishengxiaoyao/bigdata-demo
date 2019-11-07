package com.edu.bigdata.offline

import breeze.numerics.sqrt
import com.edu.bigdata.offline.model.{MongoConfig, ProductRating}
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ALSTrainer {
  def main(args: Array[String]): Unit = {

    // 定义 MongoDB 中存储的表名
    val MONGODB_RATING_COLLECTION = "Rating"
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ALSTrainer")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 MongoDB 中的数据加载进来，并转换为 RDD，之后进行 map 遍历转换为 RDD（样例类是 spark mllib 中的 Rating），并缓存
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(productRating => Rating(productRating.userId, productRating.productId, productRating.score))
      .cache()

    // ratingRDD: RDD[Rating(user, product, rating)]
    // 将一个 RDD 随机切分成两个 RDD，用以划分训练集和测试集
    val splits = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingDataRDD = splits(0)
    val testinggDataRDD = splits(1)

    // 输出最优参数
    adjustALSParams(trainingDataRDD, testinggDataRDD)

    // 关闭 Spark
    spark.close()
  }

  /**
    * 输出最优参数的方法：输入一组训练数据和测试数据，输出计算得到最小 RMSE 的那组参数
    *
    * @param trainingDataRDD
    * @param testinggData
    */
  def adjustALSParams(trainingDataRDD: RDD[Rating], testinggData: RDD[Rating]) = {
    // 这里指定迭代次数为 10，rank 和 lambda 在几个值中选取调整
    val result = for (rank <- Array(50, 100, 150, 200); lambda <- Array(1, 0.1, 0.01, 0.001))
      yield { // yield 表示把 for 循环的每一次中间结果保存下来
        val model = ALS.train(trainingDataRDD, rank, 10, lambda)
        val rmse = getRMSE(model, testinggData)
        (rank, lambda, rmse)
      }

    // 按照 rmse 排序
    // println(result.sortBy(_._3).head)
    println(result.minBy(_._3))
  }

  /**
    * 计算 RMSE
    *
    * @param model
    * @param testinggDataRDD
    */
  def getRMSE(model: MatrixFactorizationModel, testinggDataRDD: RDD[Rating]) = {
    // 将 三元组数据 转化为 二元组数据
    // testinggDataRDD: RDD[Rating(userId, productId, rating)]
    val userProductsRDD = testinggDataRDD.map(rating => (rating.user, rating.product))

    // 执行模型预测，获取预测评分矩阵
    // predictRatingRDD: RDD[Rating(userId, productId, rating)]
    val predictRatingRDD = model.predict(userProductsRDD)

    // 测试数据的真实评分
    val realRDD = testinggDataRDD.map(rating => ((rating.user, rating.product), rating.rating))
    // 测试数据的预测评分
    val predictRDD = predictRatingRDD.map(rating => ((rating.user, rating.product), rating.rating))

    // 计算 RMSE（测试数据的真实评分 与 测试数据的预测评分 做内连接操作）
    sqrt(
      realRDD.join(predictRDD).map {
        case ((userId, productId), (real, predict)) =>
          // 真实值和预测值之间的差
          val err = real - predict
          err * err
      }.mean()
    )
  }

}
