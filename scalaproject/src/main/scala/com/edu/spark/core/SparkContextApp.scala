package com.edu.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextApp {
  def main(args: Array[String]): Unit = {
    /**
      * 在启动的spark应用程序时，程序会对appName和master检查是否设置，如果没有设置报错
      * if (!_conf.contains("spark.master")) {
      * throw new SparkException("A master URL must be set in your configuration")
      * }
      * if (!_conf.contains("spark.app.name")) {
      * throw new SparkException("An application name must be set in your configuration")
      * }
      * 在idea中必须要设置appName，在shell中不用设置，必须要设置master
      * def setMaster(master: String): SparkConf = {
      * set("spark.master", master)
      * }
      * def setAppName(name: String): SparkConf = {
      * set("spark.app.name", name)
      * }
      * 在编写程序时，不要对appName和master进行硬编码。
      */
    val sparkConf = new SparkConf().setAppName("SparkContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    //将本地集合变成一个RDD，分片默认值为2
    /*val rdd=sc.parallelize(Array(1,2,3,4,5))
    rdd.collect().foreach(println)*/
    //textFile需要所有的节点上在相同的路径拥有相同的文件
    val textRdd = sc.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    //由于collect操作返回是一个Array，如果需要获取结果，需要对其进行遍历，foreach也可以认为是一个action
    textRdd.collect().foreach(println)
    /**
      * 从HDFS或者本地文件系统中读取文本文件，返回值为KV形式的(文件名称，内容)
      */
    val otherRdd = sc.wholeTextFiles("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    otherRdd.collect().foreach(println)
    otherRdd.cache()

    //textRdd.map(x=>x.split(","))
    //textRdd.flatMap(x=>x.split(",")).collect().foreach(println)
    //val peopleRdd=sc.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/data.txt")
    // peopleRdd.flatMap(x=>x.split(" ")).collect().foreach(println)
    // peopleRdd.flatMap(x=>x.split(" ")).map((_,1)).mapValues("x:"+_).collect().foreach(println)
    //var a=sc.parallelize(1 to 10)
    //a.take(3).foreach(println)
    //a.collect()
    //a.first()
    //a.top(3)
    //a.max()
    //a.min()
    //a.sortBy()
    //sc.textFile("").flatMap(_.split("\t")).map((_,1)).reduceByKey(_+_).collect()
    sc.stop()
  }
}
