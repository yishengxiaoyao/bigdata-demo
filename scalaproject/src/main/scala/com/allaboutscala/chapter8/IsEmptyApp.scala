package com.allaboutscala.chapter8

object IsEmptyApp extends App{
  /**
    * 这个collection是否为空
    */
  println("Step 1: How to initialize a Sequence of donuts")
  val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut")
  println(s"Elements of donuts = $donuts")
  println("\nStep 2: How to find out if a sequence is empty using isEmpty function")
  println(s"Is donuts sequence empty = ${donuts.isEmpty}")

  println("\nStep 3: How to create an empty sequence")
  val donuts2: Seq[String] = Seq.empty[String]
  println(s"Elements of donuts2 = $donuts2")

  println("\nStep 4: How to find out if a sequence is empty using isEmpty function")
  println(s"Is donuts2 sequence empty = ${donuts2.isEmpty}")

}
