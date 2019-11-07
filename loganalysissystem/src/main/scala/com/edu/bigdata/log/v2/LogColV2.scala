package com.edu.bigdata.log.v2

import java.net.URI

import com.edu.bigdata.log.utils.LogParser
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object LogColV2 extends Logging{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("local[3]")
      .appName("LogColV2").getOrCreate()

    val fileSystem = FileSystem.get(new URI("hdfs://localhost:8020"),
      spark.sparkContext.hadoopConfiguration)



//val time = "2019040510"  // crontab scheduler

    try {
      val time = "2019040513"  // loadtime  基准时间  spark处理的时间
      val day = time.substring(0,8)
      val hour = time.substring(8,10)
      val input = s"hdfs://localhost:8020/f12/raw/access/$time/*"
      val output = s"hdfs://localhost:8020/f12/col/accesscolv2"

      // 1: 死去活来
//      val coalesceSize = 50
//      val coalesce = FileUtils.makeCoalesce(fileSystem, input, coalesceSize)
//
//      logWarning("~~~~~~~~" + coalesce) // 3

      var logDF = spark.read.format("text").load(input) //.coalesce(coalesce)

      logDF = spark.createDataFrame(logDF.rdd.map(x => {
        LogParser.parseLog(x.getString(0))
      }),LogParser.struct)


      // step1: etl
      logDF.write.option("compression","none")
        .format("parquet")
        .mode(SaveMode.Overwrite) //  append  overwrite ==> result error
        .partitionBy("day", "hour") // k=v
        .save(output + s"/temp/$time")  //输出到temp

      // step2: move

      // step3: partition

    } catch {
      case e:Exception => e.printStackTrace()
    }

//    Thread.sleep(20000)

    spark.stop()
  }

}
