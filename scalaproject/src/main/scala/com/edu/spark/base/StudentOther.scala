package com.edu.spark.base

class StudentOther extends PersonOther {
  override val name: String = "xiaoyao"

  override def hit(): Unit = {
    println("fighting")
  }
}
