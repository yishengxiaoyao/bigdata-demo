package com.edu.bigdata.log.v1

import com.edu.bigdata.log.utils.LogParser
import org.apache.spark.sql.{SaveMode, SparkSession}

object LogCol {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[2]")
      .appName("LogCol").getOrCreate()

//val time = "2019040510"  // crontab scheduler

    val time = "2019040511"  // loadtime  基准时间  spark处理的时间
    val day = time.substring(0,8)
    val hour = time.substring(8,10)
    val input = s"hdfs://localhost:8020/f12/raw/access/$time"
    val output = s"hdfs://localhost:8020/f12/col/accesscol/day=$day/hour=$hour"

    var logDF = spark.read.format("text").load(input)

    logDF = spark.createDataFrame(logDF.rdd.map(x => {
      LogParser.parseLog(x.getString(0))
    }),LogParser.struct)

//    logDF.show(false)

    logDF.write.option("compression","none")
      .format("parquet")
      .mode(SaveMode.Overwrite).save(output)

    spark.stop()
  }

}
