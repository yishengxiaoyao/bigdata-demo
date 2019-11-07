package com.edu.bigdata.itemcf

import com.edu.bigdata.itemcf.model.{MongoConfig, ProductRating, ProductRecs, Recommendation}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object ItemCFRecommender {
  // 定义 MongoDB 中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 定义 MongoDB 中推荐表的名称
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"

  val MAX_RECOMMENDATION = 10

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ItemCFRecommender")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 MongoDB 中的数据加载进来，得到 DF (userId, productId, count)
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .map(productRating => (productRating.userId, productRating.productId, productRating.score))
      .toDF("userId", "productId", "score")
      .cache()

    // TODO: 核心算法：基于物品的协同过滤推荐（相似推荐）--计算物品的同现相似度，得到商品的相似度列表
    // 1、统计每个商品的评分个数，使用 ratingDF 按照 productId 做 groupBy，得到 (productId, count)
    val productRatingCountDF = ratingDF.groupBy("productId").count()

    // 2、在原有的 rating 表中添加一列 count，得到新的 评分表，将 ratingDF 和 productRatingCountDF 做内连接 join 即可，得到 (productId, userId, score, count)
    val ratingWithCountDF = ratingDF.join(productRatingCountDF, "productId")

    // 3、将 新的评分表 按照 userId 做两两 join，统计两个商品被同一个用户评分过的次数，得到 (userId, productId1, score1, count1, productId2, score2, count2)
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId", "productId1", "score1", "count1", "productId2", "score2", "count2") // 设置 DF 的列名称
      .select("userId", "productId1", "count1", "productId2", "count2") // 设置 DF 显示的列

    // 创建一个名为 joined 的临时表，用于写 sql 查询
    joinedDF.createOrReplaceTempView("joined")

    // 4、按照 productId1, productId2 做 groupBy，统计 userId 的数量，得到对两个商品同时评分的人数
    val sql =
      """
        |select productId1
        |, productId2
        |, count(userId) as cooCount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by productId1, productId2
      """.stripMargin

    val cooccurrenceDF = spark.sql(sql).cache() // (productId1, productId2, cooCount, count1, count2)

    val simDF = cooccurrenceDF
      .map {
        row =>
          val coocSim = cooccurrenceSim(row.getAs[Long]("cooCount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
          (row.getInt(0), (row.getInt(1), coocSim))
      }
      .rdd
      .groupByKey()
      .map {
        case (productId, recs) =>
          ProductRecs(productId, recs.toList.filter(x => x._1 != productId).sortWith(_._2 > _._2).take(MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      } // productSimGroupRDD: RDD[(productId, Seq[(productId, consinSim)])]
      .toDF()

    // 测试
    // simDF.show()

    // 将 DF 数据写入 MongoDB 数据库对应的表中
    storeDFInMongoDB(simDF, ITEM_CF_PRODUCT_RECS)

    spark.stop()
  }

  /**
    * 计算同现相似度
    *
    * @param cooCount
    * @param count1
    * @param count2
    */
  def cooccurrenceSim(cooCount: Long, count1: Long, count2: Long) = {
    cooCount / math.sqrt(count1 * count2)
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
