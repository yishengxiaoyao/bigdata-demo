package com.edu.spark.collection

object CollectionApp {
  def main(args: Array[String]): Unit = {
    var a = Map("name" -> "xiaoyao", "age" -> 30) //不可以修改值
    var b = scala.collection.mutable.Map("name" -> "xiaoyao", "age" -> 30)
    println(b.getOrElse("gender", "unknown")) //如果没有key，可以输出后面的值
    println(b.get("gender"))
    println(b("name"))
    //遍历
    for ((key, value) <- b) {
      println(key + ":" + value)
    }
    for ((key, _) <- b) {
      println(key + ":" + b.getOrElse(key, 0))
    }
    for (key <- b.keySet) {
      println(key + ":" + b.getOrElse(key, 0))
    }
    for (value <- b.values) {
      println(value)
    }
  }
}
