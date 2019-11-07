package com.allaboutscala.chapter8

object DropApp extends App {
  /**
    * drop 删除几个元素
    */
  println("Step 1: How to initialize a Sequence of donuts")
  val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut")
  println(s"Elements of donuts = $donuts")
  println("\nStep 2: How to drop the first element using drop function")
  println(s"Drop the first element in the sequence = ${donuts.drop(1)}")

  println("\nStep 3: How to drop the first two elements using the drop function")
  println(s"Drop the first and second elements in the sequence = ${donuts.drop(2)}")

}
