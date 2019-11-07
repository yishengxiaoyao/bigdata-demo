package com.edu.spark.base

object AbstractApp {

  def main(args: Array[String]): Unit = {
    var student = new StudentOther()
    println(student.hit())
    println(student.name)
  }

}
