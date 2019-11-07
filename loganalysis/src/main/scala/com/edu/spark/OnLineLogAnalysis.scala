package com.edu.spark

import java.util.regex.Pattern

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.influxdb.{InfluxDB, InfluxDBFactory}
import org.json.JSONObject

import scala.Some

/**
  *
  * 主要使用spark streaming and spark sql来实现:
  * 1.从kafka0.10 cluster读取json格式的log ，其格式: 机器名称 服务名称 时间 日志级别 日志信息
  * {"time":"2017-04-11 22:40:47,981","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:PacketResponder: BP-469682100-172.16.101.55-1489763711932:blk_1073775313_34497, type=HAS_DOWNSTREAM_IN_PIPELINE terminating"}
  * {"time":"2017-04-11 22:40:48,120","logtype":"INFO","loginfo":"org.apache.hadoop.hdfs.server.datanode.DataNode:Receiving BP-469682100-172.16.101.55-1489763711932:blk_1073775314_34498 src: /172.16.101.59:49095 dest: /172.16.101.60:50010"}
  *
  * 2.每隔5秒统计最近15秒出现的机器，日志级别为info,debug,warn,error次数
  *
  * 3.每隔5秒统计最近15秒出现的机器，日志信息出现自定义alert词的次数
  *
  *  1.消费kafka json数据转换为DF,然后show()
  *  2.group by语句
  *  3.写入到InfluxDB
  *  4.广播变量+更新(自定义预警关键词)
  *
  */

object OnLineLogAnalysis {
  //定义滑动间隔为5秒,窗口时间为30秒，即为计算每5秒的过去15秒的数据
  private val slide_interval = new Duration(5 * 1000)
  private val window_length = new Duration(5 * 1000)
  private val regexSpace = Pattern.compile(" ")
  private var influxDB: InfluxDB = null

  def onlineLogsAnalysis(): Unit = {
    //定义连接influxdb
    influxDB = InfluxDBFactory.connect("http://" + InfluxDBUtils.getInfluxIP + ":" + InfluxDBUtils.getInfluxPORT(true), "admin", "admin")
    var rp = InfluxDBUtils.defaultRetentionPolicy(influxDB.version())
    val sparkSession = SparkSession.builder()
      .master("yarn")
      .appName("OnLineLogAnalysis")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val streamingContext = new StreamingContext(sc, slide_interval)

    /* 2.开启checkpoint机制，把checkpoint中的数据目录设置为hdfs目录
    hdfs dfs -mkdir -p hdfs://nameservice1/spark/checkpointdata
    hdfs dfs -chmod -R 777 hdfs://nameservice1/spark/checkpointdata
    hdfs dfs -ls hdfs://nameservice1/spark/checkpointdata
     */
    streamingContext.checkpoint("hdfs://nameservice1/spark/checkpointdata")
    var kafkaParam = Map[String, String]("bootstrap.servers" -> "192.168.0.85:9092,192.168.0.86:9092,192.168.0.87:9092",
      "key.deserializer" -> StringDeserializer.class,
    "value.deserializer" -> StringDeserializer.class,
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> false
    )
    //3.创建要从kafka去读取的topic的集合对象
    val topics = Array("onlinelogs")
    //4.输入流
    val lines = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParam))
    //5.将DS的RDD解析为JavaDStream<CDHRoleLog>
    val cdhRoleLogFilterDStream: DStream[Option[CDHRoleLog]] = lines.map(line => {
      if (line.value().contains("INFO") == true ||
        line.value().contains("WARN") == true ||
        line.value().contains("ERROR") == true ||
        line.value().contains("FATAL") == true) {
        try {
          val jsonlogline = new JSONObject(line.value())
          val cdhRoleLog = CDHRoleLog(jsonlogline.getString("hostname"),
            jsonlogline.getString("servicename"),
            jsonlogline.getString("time"),
            jsonlogline.getString("logtype"),
            jsonlogline.getString("loginfo")
          )
          Some(cdhRoleLog)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
            None
          }
        }
      }else{None}
    })


  }
}
