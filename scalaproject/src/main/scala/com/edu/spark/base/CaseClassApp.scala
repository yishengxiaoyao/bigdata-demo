package com.edu.spark.base

object CaseClassApp {
  def main(args: Array[String]): Unit = {
    println(Dog("wangcai"))
    println(Dog("wangcai").name)
  }
}

case class Dog(name: String)
