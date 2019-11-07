package com.edu.bigdata.log.framework

import com.edu.bigdata.log.utils.LogParser
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.StructType

trait Job extends Serializable with Logging{
  val partition = ""
  val loadTime = ""
  val input = ""
  /**
    * 包的名称，使用反射来获取相应的包
    */
  val bizName = ""
  /**
    * 读取的文件格式
    */
  val format = "parquet"

  /**
    * 获取字段的类型
    * @return
    */
  def getStruct():StructType

  def parseLog(log:String):Row

  def etl(spark:SparkSession): Unit ={
    logWarning("pk etl....")
    val logDF = spark.read.format(format).load(input)
    spark.createDataFrame(logDF.rdd.map(x=>{
      LogParser.parseLog(x.getString(0))
    }),getStruct()).show(false)
  }



}
