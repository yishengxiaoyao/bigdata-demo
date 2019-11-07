package com.edu.spark.base

object SimpleObjectApp {
  def main(args: Array[String]): Unit = {
    var person = new Person()
    person.name = "doudou"
    println(person.coding())
    println(person.doing())
    println(person.printInfo())
  }
}
