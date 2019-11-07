package com.edu.spark.collection

object OptionApp {
  def main(args: Array[String]): Unit = {
    var m = Map(1 -> 2)
    println(m(1))
    println(m.get(1).get) //some(2) 类型和值 some(2).get 获取值
    println(m.get(2).getOrElse(0)) //出现异常值,异常时设置的值
    //option 是some和none的父类，some是

  }
}
