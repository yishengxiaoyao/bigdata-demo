package com.practice

/**
  * 伴生对象
  * 从上往下执行，跳过方法，在执行完其他之后，然后在调用指定的方法
  */
object ApplyTest {
  println("ApplyTest Object enter")
  var count = 0

  def incr() = {
    count = count + 1
    count
  }

  def static() = {
    println("ApplyTest object static")
  }

  println("ApplyTest Object leave")

  def apply(): Unit = {
    println("ApplyTest Object apply")
    new ApplyTest()
  }

  /*def apply(): ApplyTest = {
    println("ApplyTest Object apply")
    new ApplyTest()
  }*/

}

/**
  * 伴生类
  */
class ApplyTest {

  println("ApplyTest class enter")

  def test(): Unit = {
    println("ApplyTest class test")
  }

  def apply(): Unit = {
    println("ApplyTest class apply")
  }


  println("ApplyTest class leave")

  override def toString: String = "applytest class tostring"
}