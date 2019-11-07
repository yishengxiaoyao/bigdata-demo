package com.edu.spark.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import scala.util.Random

object SparkJob extends Logging{
  def main(args: Array[String]): Unit = {
    val spark=SparkSession.builder().appName("SparkJob").master("local[2]").getOrCreate()
    spark.sparkContext.textFile("")
      .flatMap(_.split("\t"))
      .map((_,1)).map(x=>{
      val random=new Random().nextInt(10)
      (random+"_"+x._1,x._2)
    }).reduceByKey(_+_)
      .map(x=>{
        val word=x._1.split("\t")(1)
        (word,x._2)
      }).reduceByKey(_+_)
  }
}
