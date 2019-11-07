package com.allaboutscala.chapter8

object LastApp extends App{
  /**
    * 最后一个元素
    */
  println("Step 1: How to initialize a Sequence of donuts")
  val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut")
  println(s"Elements of donuts = $donuts")

  println("\nStep 2: How to access the last element of the donut sequence by index")
  println(s"Last element of donut sequence = ${donuts(donuts.size - 1)}")

  println("\nStep 3: How to access the last element of the donut sequence by using the last function")
  println(s"Last element of donut sequence = ${donuts.last}")

  println("\nStep 4: How to create an empty sequence")
  val donuts2: Seq[String] = Seq.empty[String]
  println(s"Elements of donuts2 = $donuts2")

  println("\nStep 5: How to access the last element of the donut sequence using the lastOption function")
  println(s"Last element of empty sequence = ${donuts2.lastOption.getOrElse("No donut was found!")}")
}
