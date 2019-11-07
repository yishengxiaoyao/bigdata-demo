package com.edu.bigdata.producer

import java.io.PrintWriter
import java.text.DecimalFormat
import java.util.Date

import org.apache.commons.lang3.time.FastDateFormat

import scala.util.control._

object ProductLog {
  /**
    * 初始化数据
    */
  val startTime = "2017-01-01"
  val endTime = "2017-12-31"
  val phoneList: List[String] = List("13242820024", "14036178412", "16386074226", "13943139492", "18714767399", "14733819877", "13351126401", "13017498589", "16058589347", "18949811796", "13558773808", "14343683320", "13870632301", "13465110157", "15382018060", "13231085347", "13938679959", "13779982232", "18144784030", "18637946280")
  val phoneNameMap: Map[String, String] = Map("13242820024" -> "李雁", "14036178412" -> "卫艺", "16386074226" -> "仰莉", "13943139492" -> "陶欣悦", "18714767399" -> "施梅梅", "14733819877" -> "金虹霖", "13351126401" -> "魏明艳", "13017498589" -> "华贞", "16058589347" -> "华啟倩", "18949811796" -> "仲采绿", "13558773808" -> "卫丹", "14343683320" -> "戚丽红", "13870632301" -> "何翠柔", "13465110157" -> "钱溶艳", "15382018060" -> "钱琳", "13231085347" -> "缪静欣", "13938679959" -> "焦秋菊", "13779982232" -> "吕访琴","18144784030" -> "沈丹","18637946280" -> "褚美丽")
  def randomBuildTime(startTime:String,endTime:String): String ={
    var result:String = ""
    try{
      val dateFormt:FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd")
      val start = dateFormt.parse(startTime)
      val end = dateFormt.parse(endTime)
      if (start.getTime < end.getTime){
        val randomTS:Long = start.getTime + ((end.getTime-start.getTime)*Math.random()).toLong
        val date = new Date(randomTS)
        val otherDateFormat:FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
        result = otherDateFormat.format(date)
      }
    }catch {
      case e:Exception => println("1232141")
    }
    result
  }

  def productLog():String= {
       var caller: String = ""
    var callee: String = ""
    var callerName: String = ""
    var calleeName: String = ""
    val callerIndex: Int = (Math.random() * phoneList.size).toInt
    caller = phoneList(callerIndex)
    callerName = phoneNameMap.get(caller).getOrElse("")
    val loop = new Breaks
    loop.breakable{
      while (true) {
        val calleeIndex = (Math.random() * phoneList.size).toInt
        callee = phoneList(calleeIndex)
        calleeName = phoneNameMap.get(callee).getOrElse("")
        if (!callee.equalsIgnoreCase(caller)) {
            loop.break()
        }
      }
    }

    val randomTime:String = randomBuildTime(startTime,endTime)
    val df:DecimalFormat = new DecimalFormat("0000")
    val duration = df.format((Math.random()*30*60).toInt)
    caller+","+callee+","+randomTime+","+duration
  }

  def writeLog(filePath:String): Unit ={
    val writer = new PrintWriter(filePath)
    while(true) {
      writer.write(productLog())
    }
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty|| args.length==0){
      System.exit(0)
    }
    writeLog(args(0))
  }
}
