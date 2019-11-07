package com.edu.spark.sql

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object LogApp {

  def main(args: Array[String]): Unit = {
    /**
      * 1.读取文件
      * 2.处理每一行数据
      * 3.
      */
    var sparkConf = new SparkConf().setMaster("local[2]").setAppName("LogApp")
    var sc = new SparkContext(sparkConf)
    var lines = sc.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/data.txt")
    //要注意异常处理
    //1.统计每个域名的流量和，获取域名和流量这2个字段
    /*lines.map(x=>{
      val temp=x.split("\t")
      var domain=temp(10)
      var response=0L
      try {
        response=temp(19).toLong
      }catch {
        case e:Exception=>println(".....")
      }
      (domain,response)  //需要判断一下是否为数值类型，如果为数值类型，要进行转换
    }).reduceByKey(_+_).collect().foreach(println)*/

    //4.HDFS文件系统
    val configuration = new Configuration()
    val uri = new URI("hdfs://hadoop000:8020")
    val fileSystem = FileSystem.get(uri, configuration, "hadoop")
    if (fileSystem.exists(new Path(args(1)))) {
      //如果文件存在，直接删除
      fileSystem.delete(new Path(args(1)), true)
    }
    //2.访问次数最多的url
    //lines.map(x=>x.split("\t")(12)).take(10).foreach(println)
    lines.map(x => {
      val temp = x.split("\t")
      (temp(12), 1L)
    }).reduceByKey(_ + _).sortBy(_._2, false).saveAsTextFile("")
    //.take(10).foreach(println)
    //.collect().foreach(println)
  }

}
