package com.edu.bigdata.offline

import com.edu.bigdata.offline.model._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object OfflineRecommender {
  // 定义 MongoDB 中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 定义 MongoDB 中推荐表的名称
  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"

  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 MongoDB 中的数据加载进来，并转换为 RDD，之后进行 map 遍历转换为 三元组形式的 RDD，并缓存
    val ratingRDD = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(productRating => (productRating.userId, productRating.productId, productRating.score))
      .cache()

    // 提取出用户和商品的数据集，并去重
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // TODO: 核心计算过程 -- 基于 LFM 模型的协同过滤推荐（相似推荐）
    // 1、训练隐语义模型
    // 创建训练数据集
    val trainDataRDD = ratingRDD.map(x => Rating(x._1, x._2, x._3)) // Rating(user, product, rating)
    // rank 是模型中隐语义因子(特征)的个数, iterations 是迭代的次数, lambda 是 ALS 的正则化参数
    val (rank, iterations, lambda) = (5, 10, 0.001)
    val model = ALS.train(trainDataRDD, rank, iterations, lambda)

    // 2、获取预测评分矩阵，得到用户的商品推荐列表（用户推荐矩阵）
    // 用 userRDD 和 productRDD 做一个笛卡尔积，得到一个空的 userProductsRDD: RDD[(userId, productId)]
    val userProductsRDD = userRDD.cartesian(productRDD)
    // 执行模型预测，获取预测评分矩阵，predictRatingRDD: RDD[Rating(userId, productId, rating)]
    val predictRatingRDD = model.predict(userProductsRDD)

    // 从预测评分矩阵中提取得到用户推荐列表
    // （先过滤 filter，然后 map 转换为 KV 结构，再 groupByKey，再 map 封装样例类1，sortWith 后 take 再 map 封装样例类2）
    val userRecsDF = predictRatingRDD.filter(_.rating > 0)
      .map(
        rating =>
          (rating.user, (rating.product, rating.rating))
      )
      .groupByKey()
      .map {
        case (userId, recs) =>
          // UserRecs(userId, recs.toList.sortBy(_._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
          UserRecs(userId, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      } // userRecsRDD: RDD[(userId, Seq[(productId, score)])]
      .toDF()

    // 将 DF 数据写入 MongoDB 数据库对应的表中
    storeDFInMongoDB(userRecsDF, USER_RECS)

    // 3、利用商品的特征矩阵，计算商品的相似度列表（商品相似度矩阵）
    // 通过训练出的 model 的 productFeatures 方法，得到 商品的特征矩阵
    // 数据格式 RDD[(scala.Int, scala.Array[scala.Double])]
    val productFeaturesRDD = model.productFeatures.map {
      case (productId, featuresArray) =>
        (productId, new DoubleMatrix(featuresArray))
    }

    // 将 商品的特征矩阵 和 商品的特征矩阵 做一个笛卡尔积，得到一个空的 productFeaturesCartRDD
    val productFeaturesCartRDD = productFeaturesRDD.cartesian(productFeaturesRDD)

    // 获取 商品相似度列表（商品相似度矩阵/商品推荐列表）
    val productSimDF = productFeaturesCartRDD
      .filter { // 过滤掉自己与自己做笛卡尔积的数据
        case (a, b) =>
          a._1 != b._1
      }
      .map { // 计算余弦相似度
        case (a, b) =>
          val simScore = this.consinSim(a._2, b._2)
          // 返回一个二元组 productSimRDD: RDD[(productId, (productId, consinSim))]
          (a._1, (b._1, simScore))
      }
      .filter(_._2._2 > 0.6)
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      } // productSimGroupRDD: RDD[(productId, Seq[(productId, consinSim)])]
      .toDF()

    // 将 DF 数据写入 MongoDB 数据库对应的表中
    storeDFInMongoDB(productSimDF, PRODUCT_RECS)

    spark.stop()
  }

  /**
    * 计算两个商品之间的余弦相似度（使用的是向量点积公式）
    *
    * @param product1
    * @param product2
    * @return
    */
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double = {
    // dot 表示点积，norm2 表示模长，模长就是 L2范式
    product1.dot(product2) / (product1.norm2() * product2.norm2()) // l1范数：向量元素绝对值之和；l2范数：即向量的模长（向量的长度）
  }

  /**
    * 将 DF 数据写入 MongoDB 数据库对应的表中的方法
    *
    * @param df
    * @param collection_name
    */
  def storeDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig) = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

}
