package com.edu.bigdata.log.framework

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import scala.reflect.runtime.universe

import scala.util.Random

object SparkJob extends Logging{
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkJob").master("local[2]").getOrCreate()
    spark.sparkContext.textFile("")
      .flatMap(_.split("\t"))
      .map((_,1)).map(x=>{
      val random = new Random()
      val rand = random.nextInt(10)
      (rand+"_"+x._1,x._2)
    }).reduceByKey(_+_)
      .map(x=>{
        val word = x._1.split("\t")(1)
        (word,x._2)
      }).reduceByKey(_+_)
    spark.sql("").write.insertInto("")

    val bizName = ""
    val runtimeMirror = universe.runtimeMirror(getClass.getClassLoader)
    val module = runtimeMirror.staticModule(bizName)
    val obj = runtimeMirror.reflectModule(module)
    val job:Job = obj.instance.asInstanceOf[Job]

    job.etl(spark)

    spark.stop()
  }
}
