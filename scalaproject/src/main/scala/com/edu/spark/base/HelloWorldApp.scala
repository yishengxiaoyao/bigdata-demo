package com.edu.spark.base

object HelloWorldApp {
  def main(args: Array[String]): Unit = {
    println("Hello World!!!")
    val test: String = "abc"
    val other: String = "bcd"
    println(other.compareToIgnoreCase(test))
  }
}
