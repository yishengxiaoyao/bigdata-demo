package com.edu.bigdata.online

import com.edu.bigdata.online.model.{MongoConfig, ProductRecs}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object OnlineRecommender {
  // 定义常量和表名
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  val MONGODB_PRODUCT_RECS_COLLECTION = "ProductRecs"
  val MONGODB_RATING_COLLECTION = "Rating"

  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender",
      "kafka.topic" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OnlineRecommender")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext
    // 创建一个 StreamContext
    val ssc = new StreamingContext(sc, Seconds(2)) // 一般 500 毫秒以上

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 加载数据：加载 MongoDB 中 ProductRecs 表的数据（商品相似度列表/商品相似度矩阵/商品推荐列表）
    val simProductsMatrixMap = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_RECS_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load() // DF
      .as[ProductRecs] // DS
      .rdd // RDD
      .map { recs =>
      (recs.productId, recs.recs.map(item => (item.productId, item.score)).toMap)
    }.collectAsMap() // Map[(productId, Map[(productId, score)])]  转换成 Map 结构，这么做的目的是：为了后续查询商品相似度方便

    // 将 商品相似度 Map 广播出去
    val simProductsMatrixMapBroadCast = sc.broadcast(simProductsMatrixMap)

    // 创建到 Kafka 的连接
    val kafkaPara = Map(
      "bootstrap.servers" -> "hadoop102:9092", // 使用的是 Kafka 的高级 API
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "ECrecommender",
      "auto.offset.reset" -> "latest"
    )

    // 创建 kafka InputDStream
    val kafkaInputDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(config("kafka.topic")), kafkaPara)
    )

    // UID|PID|SCORE|TIMESTAMP
    // userId|productId|score|timestamp
    // 产生评分流
    // ratingDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(userId, productId, score, timestamp)]
    val ratingDStream = kafkaInputDStream.map {
      case msg =>
        val attr = msg.value().split("\\|")
        (attr(0).trim.toInt, attr(1).trim.toInt, attr(2).trim.toDouble, attr(3).toInt)
    }

    // TODO: 对评分流的处理流程
    ratingDStream.foreachRDD {
      rdds =>
        rdds.foreach {
          case (userId, productId, score, timestamp) =>
            println("rating data coming! >>>>>>>>>>>>>>>>>>>> ")

            // TODO: 核心实时推荐算法流程
            // 1、从 redis 中获取 当前用户最近的 K 次商品评分，保存成一个数组 Array[(productId, score)]
            val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM, userId, ConnHelper.jedis)

            // 2、从 MongoDB 的 商品相似度列表 中获取 当前商品 p 的 K 个最相似的商品列表，作为候选商品列表，保存成一个数组 Array[(productId)]
            val candidateProducts = getTopSimProducts(MAX_SIM_PRODUCTS_NUM, productId, userId, simProductsMatrixMapBroadCast.value)

            // 3、计算每一个 候选商品 q 的 推荐优先级得分，得到 当前用户的实时推荐列表，保存成一个数组 Array[(productId, score)]
            // 为什么不保存成 Recommendation 的列表呢？答：因为最后保存的过程当中不用 DataFram 的 write() 方法了，而是将每一个元素包装成 MongoDBObject 对象，然后插入列表中去
            val streamRecs = computeProductsScore(candidateProducts, userRecentlyRatings, simProductsMatrixMapBroadCast.value)

            // 4、将 当前用户的实时推荐列表数据 保存到 MongoDB
            storeDataInMongDB(userId, streamRecs)
        }
    }

    // 启动 Streaming 程序
    ssc.start()
    println(">>>>>>>>>>>>>>>>>>>> streaming started!")
    ssc.awaitTermination()
  }

  // 因为 redis 操作返回的是 java 类，为了使用 map 操作需要引入转换类
  import scala.collection.JavaConversions._

  /**
    * 1、从 redis 中获取 当前用户最近的 K 次商品评分，保存成一个数组 Array[(productId, score)]
    *
    * @param MAX_USER_RATINGS_NUM
    * @param userId
    * @param jedis
    */
  def getUserRecentlyRatings(MAX_USER_RATINGS_NUM: Int, userId: Int, jedis: Jedis) = {
    // redis 中的列表类型（list）可以存储一个有序的字符串列表
    // 从 redis 中 用户的评分队列 里获取评分数据，list 中的 键 userId:4867   值 457976:5.0
    jedis.lrange("userId:" + userId.toString, 0, MAX_USER_RATINGS_NUM)
      .map { item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt, attr(1).trim.toDouble)
      }
      .toArray
  }

  /**
    * 2、从 MongoDB 的 商品相似度列表 中获取 当前商品 p 的 K 个最相似的商品列表，作为候选商品列表，保存成一个数组 Array[(productId)]
    *
    * @param MAX_SIM_PRODUCTS_NUM
    * @param productId
    * @param userId
    * @param simProductsMatrixMap
    */
  def getTopSimProducts(MAX_SIM_PRODUCTS_NUM: Int, productId: Int, userId: Int, simProductsMatrixMap: collection.Map[Int, Map[Int, Double]])(implicit mongoConfig: MongoConfig) = {
    // 1、从广播变量 商品相似度矩阵 中拿到当前商品的相似度商品列表
    // simProductsMatrixMap: Map[(productId, Map[(productId, score)])]
    // allSimProducts: Array[(productId, score)]
    val allSimProducts = simProductsMatrixMap(productId).toArray

    // 2、定义通过 MongoDB 客户端拿到的表操作对象
    val ratingCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION)
    // 获取用户已经评分过的商品（通过 MongoDBObject 对象）
    val ratingExist = ratingCollection.find(MongoDBObject("userId" -> userId)).toArray.map(item => item.get("productId").toString.toInt)

    // 3、过滤掉用户已经评分过的商品，排序输出
    allSimProducts.filter(x => !ratingExist.contains(x._1))
      .sortWith(_._2 > _._2)
      .take(MAX_SIM_PRODUCTS_NUM)
      .map(x => x._1)
  }

  /**
    * 3、计算每一个 候选商品 q 的 推荐优先级得分，得到 当前用户的实时推荐列表，保存成一个数组 Array[(productId, score)]
    *
    * @param candidateProducts
    * @param userRecentlyRatings
    * @param simProductsMatrixMap
    */
  def computeProductsScore(candidateProducts: Array[Int], userRecentlyRatings: Array[(Int, Double)], simProductsMatrixMap: collection.Map[Int, Map[Int, Double]]) = {
    // 1、定义一个长度可变的数组 scala ArrayBuffer，用于保存每一个候选商品的基础得分
    val scores = ArrayBuffer[(Int, Double)]()

    // 2、定义两个可变的 scala HashMap，用于保存每一个候选商品的增强因子和减弱因子
    val increMap = mutable.HashMap[Int, Int]()
    val decreMap = mutable.HashMap[Int, Int]()

    // 3、对 每一个候选商品 和 每一个已经评分的商品 计算推荐优先级得分
    for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
      // 获取当前 候选商品 和当前 最近评分商品 的相似度的得分
      val simScore = getProductSimScore(candidateProduct, userRecentlyRating._1, simProductsMatrixMap)

      if (simScore > 0.6) {
        // 计算 候选商品 的基础得分
        scores += ((candidateProduct, simScore * userRecentlyRating._2))

        // 计算 增强因子 和 减弱因子
        if (userRecentlyRating._2 > 3) {
          increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
        } else {
          decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
        }
      }
    }

    // 4、根据备选商品的 productId 做 groupBy，然后根据公式最后求出候选商品的 推荐优先级得分 并排序
    scores.groupBy(_._1).map {
      case (productId, scoreList) =>
        (productId, scoreList.map(_._2).sum / scoreList.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
    }.toArray.sortWith(_._2 > _._2)
  }

  /**
    * 获取当前 候选商品 和当前 最近评分商品 的相似度的得分，得到一个 Double
    *
    * @param productId1
    * @param productId2
    * @param simProductsMatrixMap
    */
  def getProductSimScore(productId1: Int, productId2: Int, simProductsMatrixMap: collection.Map[Int, Map[Int, Double]]) = {
    simProductsMatrixMap.get(productId1) match {
      case Some(map) =>
        map.get(productId2) match {
          case Some(score) => score
          case None => 0.0
        }
      case None => 0.0
    }
  }

  /**
    * 求一个数以10为底数的对数（使用换底公式）
    *
    * @param m
    * @return
    */
  def log(m: Int): Double = {
    val N = 10
    math.log(m) / math.log(N) // 底数为 e  => ln m / ln N = log m N = lg m
  }

  /**
    * 4、将 当前用户的实时推荐列表数据 保存到 MongoDB
    *
    * @param userId
    * @param streamRecs
    */
  def storeDataInMongDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    // 到 StreamRecs 的连接
    val streaRecsCollection = ConnHelper.mongoClient(mongoConfig.db)(MONGODB_STREAM_RECS_COLLECTION)

    streaRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streaRecsCollection.insert(MongoDBObject("userId" -> userId, "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))
  }



}
