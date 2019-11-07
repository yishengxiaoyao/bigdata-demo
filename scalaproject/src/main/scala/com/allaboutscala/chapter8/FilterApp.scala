package com.allaboutscala.chapter8

object FilterApp extends App {
  /**
    * filter 匹配上
    * filterNot 匹配不上
    */
  println("Step 1: How to initialize a Sequence of donuts")
  val donuts: Seq[String] = Seq("Plain Donut", "Strawberry Donut", "Glazed Donut", "Vanilla Donut")
  println(s"Elements of donuts = $donuts")
  println("\nStep 2: How to keep only Plain and Glazed Donuts using the filter method")
  val sequenceWithPlainAndGlazedDonut = donuts.filter { donutName =>
    donutName.contains("Plain") || donutName.contains("Glazed")
  }
  println(s"Sequence with Plain and Glazed donuts only = $sequenceWithPlainAndGlazedDonut")

  println("\nStep 3: How to filter out element Vanilla Donut using the filterNot function")
  val sequenceWithoutVanillaDonut = donuts.filterNot(donutName => donutName == "Vanilla Donut" )
  println(s"Sequence without vanilla donut = $sequenceWithoutVanillaDonut")



}
