package com.allaboutscala.chapter6

import scala.collection.immutable.TreeMap

object TreeMapApp extends App {
  /**
    * 按照key进行排序
    */
  println("Step 1: How to initialize a TreeMap with 3 elements using Tuples key and value")
  val treeMap1: TreeMap[String, String] = TreeMap(("PD","Plain Donut"),("SD","Strawberry Donut"),("CD","Chocolate Donut"))
  println(s"Elements of treeMap1 = $treeMap1")

  println("\nStep 2: How to initialize TreeMap using key -> value notation")
  val treeMap2: TreeMap[String, String] = TreeMap("VD"-> "Vanilla Donut", "GD" -> "Glazed Donut")
  println(s"Elements of treeMap2 = $treeMap2")

  println("\nStep 3: How to access elements of TreeMap by specific key")
  println(s"Element by key VD = ${treeMap2("VD")}")
  println(s"Element by key GD = ${treeMap2("GD")}")

  println("\nStep 4: How to add elements to TreeMap using +")
  val treeMap3: TreeMap[String, String] = treeMap1 + ("KD" -> "Krispy Kreme Donut")
  println(s"Elements in treeMap3 = $treeMap3")

  println("\nStep 5: How to add two TreeMaps together using ++")
  val treeMap4: TreeMap[String, String] = treeMap1 ++ treeMap2
  println(s"Elements in treeMap4 = $treeMap4")

  println("\nStep 6: How to remove key and its value from TreeMap using -")
  val treeMap5: TreeMap[String, String] = treeMap4 - ("CD")
  println(s"TreeMap without the key CD and its value = $treeMap5")

  println("\nStep 7: How to change ordering of TreeMap to descending alphabet")
  object AlphabetOrdering extends Ordering[String] {
    def compare(key1:String, key2:String) = key2.compareTo(key1)
  }
  val treeMap6: TreeMap[String, String] = TreeMap(("PD","Plain Donut"),("SD","Strawberry Donut"),("CD","Chocolate Donut"))(AlphabetOrdering)
  println(s"Elements of treeMap6 in descending order = $treeMap6")

  println("\nStep 8: How to initialize an empty TreeMap")
  val emptyTreeMap: TreeMap[String,String] = TreeMap.empty[String,String]
  println(s"Empty TreeMap = $emptyTreeMap")

}
