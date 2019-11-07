package com.edu.bigdata.log.v1

import com.edu.bigdata.log.utils.LogParser
import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SaveMode, SparkSession}

object LogColShell extends Logging{

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().getOrCreate()

    //2019040510
    val time = spark.sparkContext.getConf.get("spark.loadtime","")

    if(StringUtils.isNotBlank(time)) {
      val day = time.substring(0,8)
      val hour = time.substring(8,10)

      val input = spark.sparkContext.getConf.get("spark.input",s"hdfs://localhost:8020/f12/raw/access/$time")
      val output = spark.sparkContext.getConf.get("spark.output",s"hdfs://localhost:8020/f12/col/accesscol/day=$day/hour=$hour")

      logWarning(s"time:$time ")
      logWarning(s"input:$input ")
      logWarning(s"output:$output ")

      var logDF = spark.read.format("text").load(input)

      logDF = spark.createDataFrame(logDF.rdd.map(x => {
        LogParser.parseLog(x.getString(0))
      }),LogParser.struct)

      logDF.write.option("compression","none")
        .format("parquet")
        .mode(SaveMode.Overwrite).save(output)
    }

    Thread.sleep(20000)

    spark.stop()
  }

}
