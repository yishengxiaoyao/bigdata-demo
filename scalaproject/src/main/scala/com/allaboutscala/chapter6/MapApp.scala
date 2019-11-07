package com.allaboutscala.chapter6

object MapApp extends App {
  /**
    * 使用Map(("A","B"))来初始化Map
    */
  println("Step 1: How to initialize a Map with 3 elements")
  val map1: Map[String, String] = Map(("PD","Plain Donut"),("SD","Strawberry Donut"),("CD","Chocolate Donut"))
  println(s"Elements of map1 = $map1")
  /**
    * 使用 Map("A"->"B")的方式来初始化Map
    */
  println("\nStep 2: How to initialize Map using key -> value notation")
  val map2: Map[String, String] = Map("VD"-> "Vanilla Donut", "GD" -> "Glazed Donut")
  println(s"Elements of map1 = $map2")

  /**
    * 根据key获取值
    */
  println("\nStep 3: How to access elements by specific key")
  println(s"Element by key VD = ${map2("VD")}")
  println(s"Element by key GD = ${map2("GD")}")

  /**
    * 往Map中添加元素，直接使用+ 如果是mutable，就需要使用+=
    */
  println("\nStep 4: How to add elements using +")
  val map3: Map[String, String] = map1 + ("KD" -> "Krispy Kreme Donut")
  println(s"Element in map3 = $map3")

  /**
    * 容器拼接 使用++ 如果是mutable的，使用++=
    */
  println("\nStep 5: How to add two Maps together using ++")
  val map4: Map[String, String] = map1 ++ map2
  println(s"Elements in map4 = $map4")

  /**
    * 删除一列，使用-，如果是mutable，可以使用-=
    */
  println("\nStep 6: How to remove key and its value from map using -")
  val map5: Map[String, String] = map4 - ("CD")
  println(s"Map without the key CD and its value = $map5")

  /**
    * 初始化一个空的Map
    */
  println("\nStep 7: How to initialize an empty Map")
  val emptyMap: Map[String,String] = Map.empty[String,String]
  println(s"Empty Map = $emptyMap")

}
