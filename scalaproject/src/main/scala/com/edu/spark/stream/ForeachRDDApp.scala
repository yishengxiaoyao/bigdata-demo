package com.edu.spark.stream

import java.sql.DriverManager

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


object ForeachRDDApp {
  def main(args: Array[String]): Unit = {
    socketStream()
  }

  def socketStream(): Unit = {
    //做一个开关
    //将需要过滤的数据写到数据库中
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val results = lines.flatMap(x => x.split(",")).map((_, 1)).reduceByKey(_ + _)
    //第一种写法,基于数据连接
    results.foreachRDD(rdd => {
      //在executor端创建connection，否则会报没有序列化的错误，因为需要跨机器传输,需要使用第二种写法
      val connection = createConnection() //在driver端执行
      rdd.foreach(pair => {
        val sql = s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
        connection.createStatement().execute(sql) //执行在worker端
      })
      connection.close()
    })
    //第二种写法，
    results.foreachRDD(rdd => {
      rdd.foreach(pair => {
        //RDD中的每个元素都要创建连接，每次创建连接和销毁连接都需要时间，使用rdd.foreachPartition创建连接
        val connection = createConnection()
        val sql = s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
        connection.createStatement().execute(sql) //执行在worker端
        connection.close()
      })
    })
    //第三种写法,基于partition的连接
    results.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //每个partition创建一个连接
        val connection = createConnection()
        partition.foreach(pair => {
          val sql = s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })
    //编写一个连接池
    //第四种写法，在第三种方式中进行优化，基于静态连接
    results.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        //创建连接池
        val connection = createConnection()
        partition.foreach(pair => {
          val sql = s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
  }
}
