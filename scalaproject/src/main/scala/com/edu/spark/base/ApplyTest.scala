package com.edu.spark.base

/**
  * 互为伴生
  */
object ApplyTest {
  println("enter applytest class")
  var count = 0

  def incr() = {
    count = count + 1
    count
  }

  def apply() = {
    println("object applytest apply method")
    new ApplyTest()
  }

  def static(): Unit = {
    println("object applytest static")
  }

  println("leave applytest class")
}

class ApplyTest {

  def apply() = {
    println("class applytest apply method")
    new ApplyTest()
  }

  def test(): Unit = {
    println("class applytest test method")
  }
}