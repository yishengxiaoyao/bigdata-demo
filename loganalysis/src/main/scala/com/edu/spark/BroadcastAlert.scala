package com.edu.spark

import java.util.{Calendar, Date}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer


object BroadcastAlert {
  private val user = "root"
  private val password = "123456"
  private val url = "jdbc:mysql://192.168.0.75:3306/onlineloganalysis"
  private val altertable = "alertinfo_config"
  private var lastUpdatedAt = Calendar.getInstance.getTime //上次time

  def updateAndGet(sparkSession: SparkSession, bcAlertList: Broadcast[Traversable[String]]): Broadcast[Traversable[String]] = {
    //当前事件
    val currentDate = Calendar.getInstance().getTime
    //时间差
    val diff = currentDate.getTime - lastUpdatedAt.getTime
    var resAlertList: Broadcast[Traversable[String]] = null
    //广播告警列表不为空或者 时间差超过一小时
    if (bcAlertList == null || diff >= 10000) {
      if (bcAlertList != null) {
        bcAlertList.unpersist()
      }
      lastUpdatedAt = new Date(System.currentTimeMillis())
      //定义sqlcontext
      val sparkSql = sparkSession.sqlContext
      val alterDs = sparkSql.read.format("jdbc")
        .option("url", url)
        .option("dbtable", altertable)
        .option("user", user)
        .option("passpword", password)
        .load()
      val alertList = new ArrayBuffer[String]()
      val warnInfo = alterDs.collectAsList()
      import scala.collection.JavaConversions._
      for (row_warnInfo <- warnInfo) {
        alertList += row_warnInfo.get(0).toString()
      }
      //定义广播变量
      resAlertList = sparkSql.sparkContext.broadcast(alertList)
      resAlertList
    } else {
      bcAlertList
    }
  }
}
