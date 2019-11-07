package com.edu.spark.core

import org.apache.hadoop.io.BytesWritable
import org.apache.spark.{SparkConf, SparkContext}

object SequenceFileApp {
  def main(args: Array[String]): Unit = {
    //sequenceFile()
    dataSerialization()
  }

  def dataSerialization(): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("SequenceFileApp")
      .setMaster("local[2]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //注册自定义的类型, 如果不注册自定义类，需要整个存储这个自定义的类，这种是浪费的方式。
    //.registerKryoClasses(Array(classOf[XXX]))
    var sc = new SparkContext(sparkConf)
    /**
      * sequenceFile()版本会将数据的类型通过WritableConverter转换为Writable,在填写K时，建议填写为BytesWritable
      * BytesWritable是一个字节序列被当作key或者value。
      * 在处理的过程中，第一个参数不用处理，丢掉。
      * 在
      * 底层调用hadoopFile的方法
      */
    var file = sc.sequenceFile[BytesWritable, String]("")
    file.map(x => x._2.split(",")).map(x => (x(0), x(1))).foreach(println)
    sc.stop()
  }

  def sequenceFile(): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("SequenceFileApp")
      .setMaster("local[2]")
    var sc = new SparkContext(sparkConf)
    /**
      * sequenceFile()版本会将数据的类型通过WritableConverter转换为Writable,在填写K时，建议填写为BytesWritable
      * BytesWritable是一个字节序列被当作key或者value。
      * 在处理的过程中，第一个参数不用处理，丢掉。
      * 在
      * 底层调用hadoopFile的方法
      */
    var file = sc.sequenceFile[BytesWritable, String]("")
    file.map(x => x._2.split(",")).map(x => (x(0), x(1))).foreach(println)
    sc.stop()
  }
}
