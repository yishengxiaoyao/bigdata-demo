package com.practice

object ApplyApp {
  def main(args: Array[String]): Unit = {
    //println(ApplyTest.incr())
    //val app=new ApplyTest()
    //println(app)
    val bapp = ApplyTest() //调用object的apply方法
    println(bapp)

    /*val c = new ApplyTest
    println(c)
    c() //applytest class apply*/
  }
}
