package com.edu.spark.base

object ApplyApp {

  def main(args: Array[String]): Unit = {
    println(ApplyTest.incr())
    var a = new ApplyTest()
    println(a)
    var b = ApplyTest() //调用object的apply方法。
    println(b)
    b.test()
    var c = new ApplyTest
    println(c)
    c()
  }

}
