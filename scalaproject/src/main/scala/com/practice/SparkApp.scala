package com.practice

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

object SparkApp {

  def main(args: Array[String]): Unit = {
    /*val sparkConf = new SparkConf().setAppName(args(0)).setMaster(args(1))
    val sc = new SparkContext(sparkConf)
    //val rdd=sc.parallelize(Array(1,2,3,4,5))
    //rdd.foreach(println)
    val rdd=sc.textFile("/Users/renren/Downloads/test/people.txt")
    rdd.collect().foreach(println)

    sc.stop()*/
    //1.统计某个域名的流量和：获取域名和流量这2个字段
    val spakrConf = new SparkConf().setAppName("SparkApp").setMaster("local[2]")
    val sc = new SparkContext(spakrConf)
    //hadoop的配置
   /* val uri = new URI("hdfs://hadoop001:8200");
    val fileSystem = FileSystem.get(uri,sc.hadoopConfiguration,"hadoop")
    if (fileSystem.exists(new Path(args(1)))){
      fileSystem.delete(new Path(args(1)),true)
    }
    val rdd = sc.textFile("/Users/renren/Downloads/baidu.log")*/
    /*dd.map(x=>{
      val temp=x.split("\t")
      val domain = temp(6)
      var response = 0L
      try{
        response = temp(7).toLong
      }catch {
        case e:Exception => println("parse response error!")
      }
      (domain,response)
    }).reduceByKey(_+_).foreach(println)*/
    //获取访问量最多的url
    /*rdd.map(x=>{
      val temp = x.split("\t")
      val url = temp(8)
      (url,1L)
    }).reduceByKey(_+_).sortBy(_._2,false).take(5).foreach(println)*/
    val counter = 0
    val rddcounter = sc.parallelize(1 to 10)
    rddcounter.foreach(x=>counter+x)
    println("the final value:"+counter)
    sc.stop()
  }

}
