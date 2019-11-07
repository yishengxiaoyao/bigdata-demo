package com.edu.bigdata.log.baidu

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.internal.Logging

object DateUtils extends Logging{

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyMMddHHmm")

  def getTime(time: String) = {
    try {
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf('[') + 1, time.lastIndexOf(']')))
        .getTime
    } catch {
      case e: Exception =>
        logError(s"$time parse error: ${e.getMessage}")
        0l
    }
  }

  def getTimezone(time: String) = {
    try {
      time.substring(time.indexOf('[') + 1, time.lastIndexOf(']')).split(" ")(1)
    } catch {
      case e: Exception =>
        //        logError(s"$time parse zone error: ${e.getMessage}")
        "-"
    }
  }


  def parseToMinute(time: String) = {
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  // minute 1809220022
  def getDay(minute: String) = {
    minute.substring(0, 6)
  }

  def getHour(minute: String) = {
    minute.substring(6, 8)
  }

  def getMin(minute: String) = {
    minute.substring(8, 10)
  }

  def get5Min(minute: String) = {
    val m = minute.substring(8, 10).toInt
    if (m < 10)
      "0" + m / 5 * 5  // 00 05
    else
      "" + m / 5 * 5  // 10 15 20
  }


  def main(args: Array[String]): Unit = {

    val time = parseToMinute("[22/Sep/2018:00:22:29 +0800]")
    println(getDay(time))
    println(getHour(time))
    println(getMin(time))
    println(get5Min(time))

  }


}
