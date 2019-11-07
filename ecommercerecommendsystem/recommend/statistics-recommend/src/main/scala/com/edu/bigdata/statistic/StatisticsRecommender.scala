package com.edu.bigdata.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.edu.bigdata.statistic.model.{MongoConfig, ProductRating}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object StatisticsRecommender {
  // 定义 MongoDB 中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 定义 MongoDB 中统计表的名称
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS_SCORE = "AverageProductsScore"

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 MongoDB 中的数据加载进来，并转换为 DataFrame
    val ratingDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .toDF()

    // 创建一张名为 ratings 的临时表
    ratingDF.createOrReplaceTempView("ratings")

    // TODO: 用 sparK sql 去做不同的统计推荐结果

    // 1、历史热门商品统计（按照商品的评分次数统计）数据结构是：productId, count
    val rateMoreProductsDF = spark.sql("select productId, count(productId) as count from ratings group by productId order by count desc")
    storeDFInMongoDB(rateMoreProductsDF, RATE_MORE_PRODUCTS)

    // 2、最近热门商品统计，即统计以月为单位每个商品的评分个数（需要将时间戳转换成 yyyyMM 格式后，按照商品的评分次数统计）数据结构是：productId, count, yearmonth

    // 创建一个日期格式化工具
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")

    // 注册 UDF，将 时间戳 timestamp 转化为年月格式 yyyyMM，注意：时间戳 timestamp 的单位是 秒，而日期格式化工具中 Date 需要的是 毫秒，且 format() 的结果是 字符串，需要转化为 Int 类型
    spark.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    // 把原始的 ratings 数据转换成想要的数据结构：productId, score, yearmonth，然后创建对应的临时表
    val ratingOfYearMonthDF = spark.sql("select productId, score, changeDate(timestamp) as yearmonth from ratings")
    // 将新的数据集注册成为一张临时表
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyProductsDF = spark.sql("select productId, count(productId) as count, yearmonth from " +
      "ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
    storeDFInMongoDB(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)


    // 3、商品平均得分统计（即优质商品统计）数据结构是：productId，avg
    val averageProductsScoreDF = spark.sql("select productId, avg(score) as avg from ratings group by productId order by avg desc")
    storeDFInMongoDB(averageProductsScoreDF, AVERAGE_PRODUCTS_SCORE)

    // 关闭 Spark
    spark.stop()
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
