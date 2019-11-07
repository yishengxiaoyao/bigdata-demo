package com.edu.spark.base

class Person extends Serializable {

  var name: String = _

  var age = 12

  private[this] var gender = "male"

  def coding() = {
    name + " is coding"
  }

  def doing() = {
    println(name + " is doing something!")
  }

  def main(args: Array[String]): Unit = {
    println("hello world!")
  }

  def printInfo(): Unit = {
    println(gender)
  }
}
