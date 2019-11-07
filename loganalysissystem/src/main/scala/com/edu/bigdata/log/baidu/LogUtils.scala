package com.edu.bigdata.log.baidu

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.LongAccumulator

object LogUtils {

  // schema
  val struct = StructType(Array(
    StructField("cdn", StringType),
    StructField("region", StringType),
    StructField("node", StringType),
    StructField("type", StringType),
    StructField("level", StringType),

    StructField("time", LongType),
    StructField("useTime", IntegerType),
    StructField("remoteIp", StringType),
    StructField("remotePort", IntegerType),
    StructField("xForwordedFor", StringType),

    StructField("localIp", StringType),
    StructField("localPort", IntegerType),
    StructField("requestSize", LongType),
    StructField("domain", StringType),
    StructField("method", StringType),

    StructField("urlPara", StringType),
    StructField("path", StringType),
    StructField("version", StringType),
    StructField("range", StringType),
    StructField("contentRange", StringType),

    StructField("status", StringType),
    StructField("code", IntegerType),
    StructField("extend1", StringType),
    StructField("dataType", StringType),
    StructField("responseSize", LongType),

    StructField("requestBodySize", LongType),
    StructField("server", StringType),
    StructField("referer", StringType),
    StructField("userAgent", StringType),
    StructField("cookie", StringType),

    StructField("minute", StringType),
    StructField("d", StringType),
    StructField("h", StringType),
    StructField("m5", StringType),
    StructField("timezone", StringType)
  ))



  // log:String, 所有的，失败的
  def parseLog(log: String, error:LongAccumulator, total:LongAccumulator): Row = {
    try {
      // +1
      total.add(1L)
      val p = log.split("\t")

      val length = p.length
      if(length != 72){
        // +1
        error.add(1l)
        return Row(0)
      }

      var useTime = 0
      try{
        useTime = p(5).toInt
      } catch  {
        case _:Exception => useTime=0
      }

      val minute = DateUtils.parseToMinute(p(4))
      val time = DateUtils.getTime(p(4))
      val timezone = DateUtils.getTimezone(p(4))

      val remoteIp = "-"
      val remotePort = 0

      val localIp = "-"
      var localPort = 0

      val requestSize = p(9).toLong
      var domain = "-"
      if (p(10).indexOf(":") == -1)
        domain = p(10)
      else
        domain = p(10).substring(0, p(10).indexOf(":"))
      val responseSize = p(19).toLong
      val requestBodySize = p(20).toLong

      val status = "-"
      val code = 0

      var upstreamResponseTime = 0

      val path = p(12)

      val domain_lower = domain.toLowerCase
      var json = ""


      json = "{\"x\":\"" + p(52) + "\"," +
        "}"

      Row(
        p(0), p(1), "-", p(2), p(3),
        time, useTime, remoteIp, remotePort, p(7),
        localIp, localPort, requestSize, domain_lower, p(11),
        "-", path, p(13), p(14), p(15),
        status, code, p(17), p(18), responseSize,
        requestBodySize, p(29), p(30), p(31), p(32),
        DateUtils.getMin(minute), DateUtils.getDay(minute), DateUtils.getHour(minute), DateUtils.get5Min(minute),timezone
      )
    } catch {
      case e: Exception => e.printStackTrace()
        // +1
        error.add(1l)
        Row(0)
    }
  }

  def main(args: Array[String]): Unit = {
//    val file = Source.fromFile("/Users/rocky/Downloads/bd.log")
//
//    for(line <- file.getLines()) {
//      val row = parseLog(line)
//      println(row.toString())
//    }

  }
}
