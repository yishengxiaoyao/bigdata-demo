package com.allaboutscala.chapter6

object ImmutableList extends App {

  println("Step 1: How to initialize an immutable List with 3 elements")
  val list1: List[String] = List("Plain Donut","Strawberry Donut","Chocolate Donut")
  println(s"Elements of list1 = $list1")

  println("\nStep 2: How to access elements of immutable List at specific index")
  println(s"Element at index 0 = ${list1(0)}")
  println(s"Element at index 1 = ${list1(1)}")
  println(s"Element at index 2 = ${list1(2)}")

  /**
    * 将数据添加到尾部
    */
  println("\nStep 3: How to append elements at the end of immutable List using :+")
  val list2: List[String] = list1 :+ "Vanilla Donut"
  println(s"Append elements at the end using :+ = $list2")

  /**
    * 将数据添加到头部 意思就是 + 在哪儿，就是在哪儿添加数据
    */
  println("\nStep 4: How to prepend elements at the front of immutable List using +:")
  val list3: List[String] = "Vanilla Donut" +: list1
  println(s"Prepend elements at the front using +: = $list3")

  /**
    *  使用 :: 来添加新的数据，将第一个元素作为整体，然后将后面的数据添加到后面
    */
  println("\nStep 5: How to add two immutable lists together using ::")
  val list4: List[Any] = list1 :: list2
  println(s"Add two lists together using :: = $list4")


  /**
    * 将两个list中直接拼接到一起，成为一个List
    */
  println("\nStep 6: How to add two immutable Lists together using :::")
  val list5: List[String] = list1 ::: list2
  println(s"Add two lists together using ::: = $list5")


  /**
    * 定义一个空的List 对象，或者使用Nil
    */
  println("\nStep 7: How to initialize an empty immutable List")
  val emptyList: List[String] = List.empty[String]
  println(s"Empty list = $emptyList")

}
