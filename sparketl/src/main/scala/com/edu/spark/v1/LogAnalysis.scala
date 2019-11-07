package com.edu.spark.v1

import com.edu.spark.v1.util.DateUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

object LogAnalysis {
  def main(args: Array[String]): Unit = {
   /* val sparkSession=SparkSession.builder().master("local[2]").appName("LogAnalysis").getOrCreate()
    val loadtime=sparkSession.sparkContext.getConf.get("spark.loadtime")
    val day=DateUtils.getDay(loadtime)
    val hour=DateUtils.getHour(loadtime)
    val input=s"hdfs://192.168.55.220:8020/f12/accesslog/+$loadtime"
    val output=sparkSession.sparkContext.getConf.get("spark.output")
    val logDF=sparkSession.read.format("text").load(input).coalesce(1)
    val result=sparkSession.createDataFrame(logDF.rdd.map(x=>{
      LogParser.parseLog(x.getString(0))
    }),LogParser.struct)
    //sparkSession.sql().write.insertInto()
    result.show()*/
    //result.write.option("compression","none").format("parquet").mode(SaveMode.Append).partitionBy("day","hour").save(output)
    val sparkSession=SparkSession.builder().master("local[2]").appName("LogAnalysis").getOrCreate()
    val loadtime="2019040510"
    val day=DateUtils.getDay(loadtime)
    val hour=DateUtils.getHour(loadtime)
    val logDF=sparkSession.read.format("text").load("/Users/renren/Downloads/2019040510.log")
    val result=sparkSession.createDataFrame(logDF.rdd.map(x=>{
      LogParser.parseLog(x.getString(0))
    }),LogParser.struct)
    result.write.option("compression","none").format("parquet").mode(SaveMode.Overwrite).save("/Users/renren/Downloads/test")
    sparkSession.stop()
  }

}
