package com.edu.bigdata.recommend.dataloader

import com.edu.bigdata.recommend.dataloader.model.{MongoConfig, Product, ProductRating}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataLoader {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "D:\\learn\\JetBrains\\workspace_idea3\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\learn\\JetBrains\\workspace_idea3\\ECommerceRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"

  // 定义 MongoDB 中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 Products、Ratings 数据集加载进来
    val productRDD = sc.textFile(PRODUCT_DATA_PATH)
    // 将 prodcutRDD 装换为 DataFrame
    val productDF = productRDD.map(item => {
      // productId,name,categoryIds,amazonId,imageUrl,categories,tags
      val attr = item.split("\\^")
      // 取出数据，转换成样例类
      Product(attr(0).trim.toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingRDD = sc.textFile(RATING_DATA_PATH)
    //将 ratingRDD 转换为 DataFrame
    val ratingDF = ratingRDD.map(item => {
      // userId,prudcutId,rating,timestamp
      val attr = item.split(",")
      // 取出数据，转换成样例类
      ProductRating(attr(0).trim.toInt, attr(1).trim.toInt, attr(2).trim.toDouble, attr(3).trim.toInt)
    }).toDF()

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 将数据保存到 MongoDB 中
    storeDataInMongDB(productDF, ratingDF)

    // 关闭 Spark
    spark.stop()
  }

  /**
    * 将数据写入 MongoDB 中
    *
    * @param productDF
    * @param ratingDF
    * @param mongoConfig
    */
  def storeDataInMongDB(productDF: DataFrame, ratingDF: DataFrame)(implicit mongoConfig: MongoConfig) = {
    // 创建一个到 MongoDB 的连接客户端
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    // 定义通过 MongoDB 客户端拿到的表操作对象
    val productCollection = mongoClient(mongoConfig.db)(MONGODB_PRODUCT_COLLECTION)
    val ratingCollection = mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)

    // 如果 MongoDB 中已有对应的表，那么应该删除
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // 将当前数据写入到 MongoDB 对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对数据表创建索引
    productCollection.createIndex(MongoDBObject("productId" -> 1))
    ratingCollection.createIndex(MongoDBObject("userId" -> 1))
    ratingCollection.createIndex(MongoDBObject("productId" -> 1))

    // 关闭 MongoDB 的连接
    mongoClient.close()
  }
}
