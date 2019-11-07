package com.edu.bigdata.mock

import java.util.UUID

import com.edu.bigdata.common.model.{ProductInfo, UserInfo, UserVisitAction}
import com.edu.bigdata.common.util.{DateUtils, StringUtils}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 离线模拟数据的生成
  *
  * date：是当前日期
  * age: 0 - 59
  * professionals: professional[0 - 99]
  * cities: 0 - 99
  * sex: 0 - 1
  * keywords: ("火锅", "蛋糕", "重庆辣子鸡", "重庆小面", "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉")
  * categoryIds: 0 - 99
  * ProductId: 0 - 99
  */
object MockDataGenerate {
  /**
    * 模拟用户的行为信息
    *
    * @return
    */
  private def mockUserVisitActionData(): Array[UserVisitAction] = {
    val rows = ArrayBuffer[UserVisitAction]()
    val random = new Random()

    val searchKeywords = Array("华为手机", "联想笔记本", "小龙虾", "卫生纸", "吸尘器", "Lamer", "机器学习", "苹果", "洗面奶", "保温杯")
    // yyyy-MM-dd
    val date = DateUtils.getTodayDate()
    // 关注四个行为：搜索、点击、下单、支付
    val actions = Array("search", "click", "order", "pay")

    // 一共 100 个用户（有重复）
    for (i <- 0 until 100) {
      val userid = random.nextInt(100)
      // 每个用户产生 10 个 session
      for (j <- 0 until 10) {
        // 不可变的，全局的，独一无二的 128bit 长度的标识符，用于标识一个 session，体现一次会话产生的 sessionId 是独一无二的
        val sessionid = UUID.randomUUID().toString().replace("-", "")
        // 在 yyyy-MM-dd 后面添加一个随机的小时时间（0-23）
        val baseActionTime = date + " " + random.nextInt(23) // 2019-05-30 12
        // 每个 (userid + sessionid) 生成 0-100 条用户访问数据
        for (k <- 0 to random.nextInt(100)) {
          val pageid = random.nextInt(10)
          // 在 yyyy-MM-dd HH 后面添加一个随机的分钟时间和秒时间，2019-05-30 12:25:30
          val actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59))) + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
          var searchKeyword: String = null
          var clickCategoryId: Long = -1L
          var clickProductId: Long = -1L
          var orderCategoryIds: String = null
          var orderProductIds: String = null
          var payCategoryIds: String = null
          var payProductIds: String = null
          val cityid = random.nextInt(10).toLong

          // 随机确定用户在当前 session 中的行为
          val action = actions(random.nextInt(4))

          // 根据随机产生的用户行为 action 决定对应字段的值
          action match {
            case "search" => searchKeyword = searchKeywords(random.nextInt(10))
            case "click" => clickCategoryId = random.nextInt(100).toLong
              clickProductId = String.valueOf(random.nextInt(100)).toLong
            case "order" => orderCategoryIds = random.nextInt(100).toString
              orderProductIds = random.nextInt(100).toString
            case "pay" => payCategoryIds = random.nextInt(100).toString
              payProductIds = random.nextInt(100).toString
          }

          rows += UserVisitAction(date, userid, sessionid,
            pageid, actionTime, searchKeyword,
            clickCategoryId, clickProductId,
            orderCategoryIds, orderProductIds,
            payCategoryIds, payProductIds, cityid)
        }
      }
    }

    rows.toArray
  }

  /**
    * 模拟用户信息表
    *
    * @return
    */
  private def mockUserInfo(): Array[UserInfo] = {
    val rows = ArrayBuffer[UserInfo]()

    val sexes = Array("male", "female")
    val random = new Random()

    // 随机产生 100 个用户的个人信息
    for (i <- 0 until 100) {
      val userid = i
      val username = "user" + i
      val name = "name" + i
      val age = random.nextInt(60)
      val professional = "professional" + random.nextInt(100)
      val city = "city" + random.nextInt(100)
      val sex = sexes(random.nextInt(2))

      rows += UserInfo(userid, username, name, age, professional, city, sex)
    }

    rows.toArray
  }

  /**
    * 模拟产品数据表
    *
    * @return
    */
  private def mockProductInfo(): Array[ProductInfo] = {
    val rows = ArrayBuffer[ProductInfo]()
    val random = new Random()

    val productStatus = Array(0, 1)

    // 随机产生 100 个产品信息
    for (i <- 0 until 100) {
      val productId = i
      val productName = "product" + i
      val extendInfo = "{\"product_status\": " + productStatus(random.nextInt(2)) + "}" // 注意这里是 json 串

      rows += ProductInfo(productId, productName, extendInfo)
    }

    rows.toArray
  }

  /**
    * 将 DataFrame 插入到 Hive 表中
    *
    * @param spark     SparkSQL 客户端
    * @param tableName 表名
    * @param dataDF    DataFrame
    */
  private def insertHive(spark: SparkSession, tableName: String, dataDF: DataFrame): Unit = {
    spark.sql("DROP TABLE IF EXISTS " + tableName)
    dataDF.write.saveAsTable(tableName)
  }

  val USER_VISIT_ACTION_TABLE = "user_visit_action"
  val USER_INFO_TABLE = "user_info"
  val PRODUCT_INFO_TABLE = "product_info"

  /**
    * 主入口方法
    *
    * @param args 启动参数
    */
  def main(args: Array[String]): Unit = {

    // 创建 Spark 配置
    val sparkConf = new SparkConf().setAppName("MockData").setMaster("local[*]")

    // 创建 Spark SQL 客户端
    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 模拟数据
    val userVisitActionData = this.mockUserVisitActionData()
    val userInfoData = this.mockUserInfo()
    val productInfoData = this.mockProductInfo()

    // 将模拟数据转换为 RDD
    val userVisitActionRdd = spark.sparkContext.makeRDD(userVisitActionData)
    val userInfoRdd = spark.sparkContext.makeRDD(userInfoData)
    val productInfoRdd = spark.sparkContext.makeRDD(productInfoData)

    // 加载 SparkSQL 的隐式转换支持
    import spark.implicits._

    // 将用户访问数据转换为 DF 保存到 Hive 表中
    val userVisitActionDF = userVisitActionRdd.toDF()
    insertHive(spark, USER_VISIT_ACTION_TABLE, userVisitActionDF)

    // 将用户信息数据转换为 DF 保存到 Hive 表中
    val userInfoDF = userInfoRdd.toDF()
    insertHive(spark, USER_INFO_TABLE, userInfoDF)

    // 将产品信息数据转换为 DF 保存到 Hive 表中
    val productInfoDF = productInfoRdd.toDF()
    insertHive(spark, PRODUCT_INFO_TABLE, productInfoDF)

    spark.close()
  }
}
