package com.allaboutscala.chapter8

object CollectApp extends App {
  /**
    * collect这个函数
    */
  println("Step 1: How to initialize a Sequence which contains donut names and prices")
  val donutNamesandPrices: Seq[Any] = Seq("Plain Donut", 1.5, "Strawberry Donut", 2.0, "Glazed Donut", 2.5)
  println(s"Elements of donutNamesAndPrices = $donutNamesandPrices")

  println("\nStep 2: How to use collect function to cherry pick all the donut names")
  val donutNames: Seq[String] = donutNamesandPrices.collect{ case name: String => name }
  println(s"Elements of donutNames = $donutNames")

  println("\nStep 3: How to use collect function to cherry pick all the donut prices")
  val donutPrices: Seq[Double] = donutNamesandPrices.collect{ case price: Double => price }
  println(s"Elements of donutPrices = $donutPrices")

}
