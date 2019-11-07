package com.allaboutscala.chapter8

object IntersectApp extends App {
  /**
    * 两个set的交集
    */
  println("Step 1: How to initialize a Set of donuts")
  val donuts1: Set[String] = Set("Plain Donut", "Strawberry Donut", "Glazed Donut")
  println(s"Elements of donuts1 = $donuts1")

  println("\nStep 2: How to initialize another Set of donuts")
  val donuts2: Set[String] = Set("Plain Donut", "Chocolate Donut", "Vanilla Donut")
  println(s"Elements of donuts2 = $donuts2")
  println("\nStep 3: How to find the common elements between two Sets using intersect function")
  println(s"Common elements between donuts1 and donuts2 = ${donuts1 intersect donuts2}")
  println(s"Common elements between donuts2 and donuts1 = ${donuts2 intersect donuts1}")

  println("\nStep 4: How to find the common elements between two Sets using & function")
  println(s"Common elements between donuts1 and donuts2 = ${donuts1 & donuts2}")
  println(s"Common elements between donuts2 and donuts1 = ${donuts2 & donuts1}")

}
