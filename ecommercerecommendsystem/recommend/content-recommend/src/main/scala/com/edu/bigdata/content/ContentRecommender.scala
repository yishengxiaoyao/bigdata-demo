package com.edu.bigdata.content

import com.edu.bigdata.content.model.{MongoConfig, Product, ProductRecs, Recommendation}
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

object ContentRecommender {
  // 定义 mongodb 中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val CONTENT_PRODUCT_RECS = "ContentBasedProductRecs"

  def main(args: Array[String]): Unit = {
    // 定义用到的配置参数
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://hadoop102:27017/ECrecommender",
      "mongo.db" -> "ECrecommender"
    )

    // 创建一个 SparkConf 配置
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("ContentRecommender")
    // 创建一个 SparkSession
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    // 创建一个 sparkContext
    val sc = spark.sparkContext

    // 声明一个隐式的配置对象，方便重复调用（当多次调用对 MongoDB 的存储或读写操作时）
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    // 加入隐式转换：在对 DataFrame 和 Dataset 进行操作许多操作都需要这个包进行支持
    import spark.implicits._

    // 将 MongoDB 中的数据加载进来
    val productTagsDF = spark
      .read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .map(product => (product.productId, product.name, product.tags.map(x => if (x == '|') ' ' else x))) // 因为 TF-IDF 默认使用的分词器的分隔符是空格
      .toDF("productId", "name", "tags")
      .cache()

    // TODO: 用 TF-IDF 算法将商品的标签内容进行提取，得到商品的内容特征向量
    // 1、实例化一个分词器，用来做分词，默认按照空格进行分词（注意：org.apache.spark.ml._ 下的 API 都是针对 DF 来操作的）
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器做转换后，得到增加一个新列 words 的 DF
    val wordsDataDF = tokenizer.transform(productTagsDF)

    // 2、定义一个 HashingTF 工具，用于计算频次 TF（映射特征的过程使用的就是 Hash 算法，特征的数量就是 Hash 的分桶数量，若分桶的数量过小，会出现 Hash 碰撞，默认分桶很大，后面做笛卡尔积性能很差）
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rowFeatures").setNumFeatures(1000)
    // 用 HashingTF 做转换
    val featurizedDataDF = hashingTF.transform(wordsDataDF)

    // 3、定义一个 IDF 工具，计算 TF-IDF
    val idf = new IDF().setInputCol("rowFeatures").setOutputCol("features")
    // 训练一个 idf 模型，将词频数据传入，得到 idf 模型（逆文档频率）
    val idfModel = idf.fit(featurizedDataDF)
    // 通过 idf 模型转换后，得到增加一个新列 features 的 DF，即用 TF-IDF 算法得到新的特征矩阵
    val rescaleDataDF = idfModel.transform(featurizedDataDF)

    // 测试
    // rescaleDataDF.show(truncate = false)

    // 对数据进行转换，得到所需要的 RDD
    // 从得到的 rescaledDataDF 中提取特征向量
    val productFeaturesRDD = rescaleDataDF
      .map {
        row => // DF 转换为 二元组
          (row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray)
      }
      .rdd
      .map {
        case (productId, featuresArray) =>
          (productId, new DoubleMatrix(featuresArray))
      }

    // 将 商品的特征矩阵 和 商品的特征矩阵 做一个笛卡尔积，得到一个稀疏的 productFeaturesCartRDD
    val productFeaturesCartRDD = productFeaturesRDD.cartesian(productFeaturesRDD)

    // 测试
    // productFeaturesCartRDD.foreach(println(_))

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
    storeDFInMongoDB(productSimDF, CONTENT_PRODUCT_RECS)

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
