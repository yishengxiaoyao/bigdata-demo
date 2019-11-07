package com.edu.bigdata.log.baidu

import java.util.Date

import com.edu.bigdata.log.utils.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object CdnCol extends Logging{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val fileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val sc = spark.sparkContext
    val appName = sc.getConf.get("spark.app.name")
    val inputPath = sc.getConf.get("spark.app.inputPath")
    val outputPath = sc.getConf.get("spark.app.outputPath")
    val coalesceSize = sc.getConf.get("spark.app.coalesce.size").toInt
    val loadTime = sc.getConf.get("spark.app.loadtime")

    logInfo(s"inputPath:$inputPath")
    logInfo(s"outputPath:$outputPath")
    logInfo(s"inputPath:$inputPath")
    logInfo(s"coalesceSize:$coalesceSize")

    val begin = new Date().getTime
    try{
      val coalesce = FileUtils.makeCoalesce(fileSystem,inputPath,coalesceSize)
      logInfo(s"$loadTime: $inputPath , $coalesceSize, $coalesce")

      val erroAccum = spark.sparkContext.longAccumulator("error")
      val totalAccum = spark.sparkContext.longAccumulator("total")
      var logDF = spark.read.text(inputPath).coalesce(coalesce)

      logDF = spark.createDataFrame(logDF.rdd.map(x=>{
        LogUtils.parseLog(x.getString(0),erroAccum,totalAccum)
      }).filter(_.length!=1),LogUtils.struct)

      val errorCount = erroAccum.value
      val totalCount = totalAccum.value

      logDF.write.mode(SaveMode.Overwrite).format("orc")//.option("compression","none")
        .partitionBy("d","h","m5").save(outputPath)

      logInfo(loadTime + "[" + appName + "] 转换用时 " + (new Date().getTime - begin) + " ms")
    }catch {
      case e:Exception=>
        e.printStackTrace()
        logError("[" + appName + "] 失败 处理异常" + e.getMessage)
    }finally {
      spark.stop()
    }
  }
}
