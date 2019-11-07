package com.allaboutscala.chapter8

object HeadApp extends App {
  /**
    * head返回第一个元素
    */
  println("Step 1: How to initialize a Sequence of donuts")
  val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut")
  println(s"Elements of donuts = $donuts")
  println("\nStep 2: How to access the first element of the donut sequence")
  println(s"First element of donut sequence = ${donuts(0)}")
  println("\nStep 3: How to access the first element of the donut sequence using the head method")
  println(s"First element of donut sequence using head method = ${donuts.head}")

  println("\nStep 4: How to create an empty sequence")
  val donuts2: Seq[String] = Seq.empty[String]
  println(s"Elements of donuts2 = $donuts2")
  println("\nStep 5: How to access the first element of the donut sequence using the headOption function")
  println(s"First element of empty sequence = ${donuts2.headOption.getOrElse("No donut was found!")}")

}
