package com.edu.bigdata.advertising

import java.util.Date

import com.edu.bigdata.advertising.dao._
import com.edu.bigdata.advertising.model._
import com.edu.bigdata.common.conf.ConfigurationManager
import com.edu.bigdata.common.util.{Constants, DateUtils}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

object AdClickRealTimeStat {

  def main(args: Array[String]): Unit = {
    // 构建 Spark 上下文
    val sparkConf = new SparkConf().setAppName("streamingRecommendingSystem").setMaster("local[*]")

    // 创建 Spark 客户端
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc, Seconds(5))

    // 设置检查点目录
    ssc.checkpoint("./streaming_checkpoint")

    // 获取 kafka 的配置
    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    // kafka 消费者参数配置
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers, // 用于初始化连接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "commerce-consumer-group", // 用于标识这个消费者属于哪个消费团体(组)
      "auto.offset.reset" -> "latest", // 如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性，latest 表示自动重置偏移量为最新的偏移量
      "enable.auto.commit" -> (false: java.lang.Boolean) // 如果是 true，则这个消费者的偏移量会在后台自动提交
    )

    // 创建 DStream，返回接收到的输入数据
    // LocationStrategies：                  根据给定的主题和集群地址创建 consumer
    // LocationStrategies.PreferConsistent： 持续的在所有 Executor 之间均匀分配分区(即把 Executor 当成 Kafka Consumer)，常用该方式
    // ConsumerStrategies：                  选择如何在 Driver 和 Executor 上创建和配置 Kafka Consumer
    // ConsumerStrategies.Subscribe：        订阅一系列主题
    // adRealTimeLogDStream: DStream[RDD, RDD, RDD, ...] -> RDD[Message] -> Message: key value
    val adRealTimeLogDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam))

    // 取出 DStream 里面每一条数据的 value 值
    // adRealTimeLogDStream: DStream[RDD, RDD, RDD, ...] -> RDD[String] -> String: timestamp province city userid adid
    val adRealTimeValueDStream = adRealTimeLogDStream.map(consumerRecordRDD => consumerRecordRDD.value())

    // 用于 Kafka Stream 的线程非安全问题，重新分区切断血统
    // adRealTimeValueDStream = adRealTimeValueDStream.repartition(400)

    // 根据黑名单进行数据过滤
    // 刚刚接受到原始的用户点击行为日志之后
    // 根据 mysql 中的动态黑名单，进行实时的黑名单过滤（黑名单用户的点击行为，直接过滤掉，不要了）
    // 使用 transform 算子（将 dstream 中的每个 batch RDD 进行处理，转换为任意的其他 RDD，功能很强大）
    val adRealTimeFilterDStream = adRealTimeValueDStream.transform {
      consumerRecordRDD =>
        // 首先从 MySQL 中查询所有黑名单用户
        // adBlacklists: Array[AdBlacklist]  AdBlacklist: userId
        val adBlacklistArray = AdBlacklistDAO.findAll()

        // userIdArray: Array[Long]  [userId1, userId2, ...]
        val userIdArray = adBlacklistArray.map(item => item.userid)

        consumerRecordRDD.filter {
          // consumerRecord: timestamp province city userid adid
          case consumerRecord =>
            val consumerRecordSplit = consumerRecord.split(" ")
            val userId = consumerRecordSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }

    adRealTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))

    ssc.start()
    ssc.awaitTermination()
  }

  /**
    * 需求七：广告点击黑名单实时统计
    *
    * @param adRealTimeFilterDStream
    */
  def generateBlackListStat(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD, RDD, RDD, ...] -> RDD[String] -> String: timestamp province city userid adid
    // key2NumDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, 1L)]
    val key2NumDStream = adRealTimeFilterDStream.map {
      // consumerRecordRDD: timestamp province city userid adid
      case consumerRecordRDD =>
        val consumerRecordSplit = consumerRecordRDD.split(" ")
        val timestamp = consumerRecordSplit(0).toLong
        // dateKey: yyyyMMdd
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val userid = consumerRecordSplit(3).toLong
        val adid = consumerRecordSplit(4).toLong

        val key = dateKey + "_" + userid + "_" + adid // 组合 key

        (key, 1L)
    }

    // key2CountDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, 25)]
    val key2CountDStream = key2NumDStream.reduceByKey(_ + _)

    // 根据每一个 RDD 里面的数据，更新 用户点击次数表 数据
    key2CountDStream.foreachRDD {
      rdd => rdd.foreachPartition {
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for ((key, count) <- items) {
            // 切割数据
            val keySplit = key.split("_")

            // 取出数据
            val date = keySplit(0)
            val userid = keySplit(1).toLong
            val adid = keySplit(2).toLong

            // 封装数据，并放入 ArrayBuffer 中
            clickCountArray += AdUserClickCount(date, userid, adid, count)
          }

          // 更新 MySQl 数据库中表的数据
          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    // 对 DStream 做 filter 操作：就是遍历 DStream 中的每一个 RDD 中的每一条数据
    // key2BlackListDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, 150)]
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        // 切割数据
        val keySplit = key.split("_")

        // 取出数据
        val date = keySplit(0)
        val userid = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userid, adid)

        if (clickCount > 100) {
          true // 留下
        } else {
          false // 过滤掉
        }
    }

    // key2BlackListDStream.map: DStream[RDD[userid]]
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) =>
        key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct()) // 转换 key 并去重

    // 将结果批量插入 MySQL 数据库中
    userIdDStream.foreachRDD {
      rdd => rdd.foreachPartition {
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for (userId <- items) {
            userIdArray += AdBlacklist(userId)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }
  }

  /**
    * 需求八：各省各城市广告点击量实时统计（使用累计统计）
    *
    * @param adRealTimeFilterDStream
    */
  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD, RDD, RDD, ...] -> RDD[String] -> String: timestamp province city userid adid
    // key2NumDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, 1L)]
    val key2NumDStream = adRealTimeFilterDStream.map {
      case consumerRecordRDD =>
        val consumerRecordSplit = consumerRecordRDD.split(" ")
        val timestamp = consumerRecordSplit(0).toLong
        // dateKey: yyyyMMdd
        val dateKey = DateUtils.formatDateKey(new Date(timestamp))
        val province = consumerRecordSplit(1)
        val city = consumerRecordSplit(2)
        val adid = consumerRecordSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adid // 组合 key

        (key, 1L)
    }

    // 执行 updateStateByKey 操作（全局的累计性的操作）
    val key2StateDStream = key2NumDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        var newValue = 0L

        if (state.isDefined) {
          newValue = state.get
        }

        for (value <- values) {
          newValue += value
        }

        Some(newValue)
    }

    // 将结果批量插入 MySQL 数据库中
    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStatArray = new ArrayBuffer[AdStat]()

            for ((key, count) <- items) {
              // 切割数据
              val keySplit = key.split("_")

              // 取出数据
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong

              // 封装数据，并放入 ArrayBuffer 中
              adStatArray += AdStat(date, province, city, adid, count)
            }

            AdStatDAO.updateBatch(adStatArray.toArray)
        }
    }
  }

  /**
    * 需求九：每天每个省份 Top3 热门广告
    *
    * @param spark
    * @param key2ProvinceCityCountDStream
    */
  def provinceTop3AdverStat(spark: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {
    // key2ProvinceCityCountDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(key, count)]
    // key: date_province_city_adid

    // 转换 key
    // key2ProvinceCountDStream: DStream[RDD, RDD, RDD, ...] -> RDD[(newKey, count)]
    // newKey: date_province_adid
    val key2ProvinceCountDStream = key2ProvinceCityCountDStream.map {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adid = keySplit(3)

        val newKey = date + "_" + province + "_" + adid // 组合 key

        (newKey, count)
    }

    // 聚合新 key
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DStream = key2ProvinceAggrCountDStream.transform {
      rdd =>
        // rdd: RDD[(newKey, count)]  newKey: date_province_adid

        // 转化为新的 RDD
        val basicDataRDD = rdd.map {
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adid = keySplit(2).toLong

            (date, province, adid, count)
        }

        // 将 RDD 转化为 DF
        import spark.implicits._
        basicDataRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("temp_basic_info")

        // 使用 Spark SQL 执行 SQL 语句，配合开窗函数，统计出各省份 top3 热门的广告
        val sql = "select date, province, adid, count from (" +
          "select date, province, adid, count, " +
          "row_number() over(partition by date, province order by count desc) row_number from temp_basic_info) t " +
          "where row_number <= 3"

        spark.sql(sql).rdd
    }

    top3DStream.foreachRDD {
      // rdd: RDD[Row]
      rdd =>
        rdd.foreachPartition {
          // items: Row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()

            for (item <- items) {
              // 取出数据
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adid = item.getAs[Long]("adid")
              val count = item.getAs[Long]("count")

              // 封装数据
              top3Array += AdProvinceTop3(date, province, adid, count)
            }

            // 写入 MySQL 数据库中
            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }
  }

  /**
    * 需求十：计算最近 1 小时滑动窗口内的广告点击趋势
    *
    * @param adRealTimeFilterDStream
    * @return
    */
  def recentHourAdverClickStat(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map {
      // consumerRecordRDD: timestamp province city userid adid
      case consumerRecordRDD =>
        val consumerRecordSplit = consumerRecordRDD.split(" ")
        val timestamp = consumerRecordSplit(0).toLong

        // timeMinute: yyyyMMddHHmm
        val timeMinute = DateUtils.formatTimeMinute(new Date(timestamp))
        val adid = consumerRecordSplit(4).toLong

        val key = timeMinute + "_" + adid // 组合 key

        (key, 1L)
    }

    // 第一个 Minutes 表示 窗口大小，第二个 Minutes 表示 窗口滑动步长
    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a: Long, b: Long) => (a + b), Minutes(60), Minutes(1))

    key2WindowDStream.foreachRDD {
      rdd => rdd.foreachPartition {
        // items: (key, count)
        items =>
          val trendArray = ArrayBuffer[AdClickTrend]()

          for ((key, count) <- items) {
            val keySplit = key.split("_")

            // 切割数据
            // timeMinute: yyyyMMddHHmm
            val timeMinute = keySplit(0)

            // 获取数据
            val date = timeMinute.substring(0, 8)   // 包头不包尾，注意是索引
            val hour = timeMinute.substring(8, 10)  // 包头不包尾，注意是索引
            val minute = timeMinute.substring(10)   // 包头不包尾，注意是索引

            val adid = keySplit(1).toLong

            // 封装数据
            trendArray += AdClickTrend(date, hour, minute, adid, count)
          }

          // 写入 MySQL 数据库中
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }
  }


}
