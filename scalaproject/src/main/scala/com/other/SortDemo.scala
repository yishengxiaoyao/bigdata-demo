package com.other

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    val data: RDD[String] = sc.parallelize(List("wx 28 85 wx.zfb.com", "pp 30 85", "dd 18 100", "taoge 35 100", "laozhao 30 120", "huge 26 9999"))
    val prdd:RDD[Person] = data.map(t=>{
      val temp = t.split(" ")
      val name = temp(0)
      val age = temp(1).toInt
      val fv = temp(2).toInt
      new Person(name,age,fv)
    })

    val sortedrdd:RDD[Person] = prdd.sortBy(t=>t)
    sortedrdd.foreach(println)
  }
}

class Person(val name: String, val age: Int, val fv: Int)
  extends Serializable
    with Ordered[Person] {
  override def compare (that: Person): Int = {
  //    根据颜值降序   如果颜值相同  再按照年龄的升序
    if (this.fv == that.fv) {
    this.age - that.age
    } else {
      that.fv - this.fv
    }
  }
  override def toString: String = s"$name,$age,$fv"
}
