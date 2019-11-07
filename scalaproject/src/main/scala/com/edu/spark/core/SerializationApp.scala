package com.edu.spark.core

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object SerializationApp {
  def main(args: Array[String]): Unit = {
    var sparkConf = new SparkConf()
      .setAppName("SequenceFileApp")
      .setMaster("local[2]")
      //在编写shell时，可以使用 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.registerKryoClasses(Array(classOf[Student]))
    var sc = new SparkContext(sparkConf)
    val students = ListBuffer[Student]()
    for (i <- 1 to 1000000) {
      students.append(Student(i, "eduspark_" + i, "30"))
    }
    var studentRdd = sc.parallelize(students)
    studentRdd.persist(StorageLevel.MEMORY_ONLY_SER)
    studentRdd.count()
    Thread.sleep(20000)
    sc.stop()
  }
}

case class Student(id: Int, name: String, age: String)
