package com.edu.bigdata.log.framework
import com.edu.bigdata.log.utils.DateUtils
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types._

object HourJob extends Job {

  override val input: String = ""
  /**
    * 读取的文件格式
    */
  override val format: String = ""
  override val partition: String = "day,hour"

  /**
    * 获取字段的类型
    */
  override def getStruct(): StructType =  {
    StructType(Array(
      StructField("id", IntegerType),
      StructField("name", StringType),
      StructField("salary", DoubleType),
      StructField("time", LongType),
      StructField("day", StringType),  //分区字段
      StructField("hour", StringType)   //分区字段
    ))
  }


  override def parseLog(log: String): Row = {
    try{
      val splits = log.split("\t")
      val id = splits(0).toInt
      val name = splits(1)
      val salary = splits(3).toDouble

      val time = DateUtils.getTime(splits(2))
      val minute = DateUtils.parseToMinute(splits(2))
      val day = DateUtils.getDay(minute)
      val hour = DateUtils.getHour(minute)
      Row(id,name,salary,time,day,hour)
    }catch {
      case e:Exception =>
        e.printStackTrace()
        Row(0)
    }
  }
}