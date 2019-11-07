package com.edu.spark.v1.util

import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import org.apache.spark.internal.Logging

object DateUtils extends Logging{

  //输入文件日期时间格式
  val SOURCE_TIME_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.ENGLISH)


  //目标日期格式
  val TARGET_TIME_FORMAT = new SimpleDateFormat("yyyyMMddHHmmss")


  def getTime(time: String) = {
    try {
      SOURCE_TIME_FORMAT.parse(time).getTime
    } catch {
      case e: Exception =>
        logError(s"$time parse error: ${e.getMessage}")
        0l
    }
  }

  def parseToMinute(time: String) = {
    TARGET_TIME_FORMAT.format(new Date(getTime(time)))
  }

  def getDay(minute: String) = {
    minute.substring(0, 8)
  }

  def getHour(minute: String) = {
    minute.substring(8, 10)
  }

  def main(args: Array[String]): Unit = {
    println(parseToMinute("2019-04-05 10:14:09"))
    println(getDay(parseToMinute("2019-04-05 10:14:09")))
    println(getHour(parseToMinute("2019-04-05 10:14:09")))
  }
}
