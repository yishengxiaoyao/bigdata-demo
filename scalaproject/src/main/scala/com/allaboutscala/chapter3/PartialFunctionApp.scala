package com.allaboutscala.chapter3

object PartialFunctionApp extends App {
  println("Step 1: Review of Pattern Matching in Scala")
  val donut = "Glazed Donut"
  val tasteLevel = donut match {
    case "Glazed Donut" | "Strawberry Donut" => "Very tasty"
    case "Plain Donut" => "Tasty"
    case _  => "Tasty"
  }
  println(s"Taste level of $donut = $tasteLevel")



  println("\nStep 2: How to define a Partial Function named isVeryTasty")
  val isVeryTasty: PartialFunction[String, String] = {
    case "Glazed Donut" | "Strawberry Donut" => "Very Tasty"
  }



  println("\nStep 3: How to call the Partial Function named isVeryTasty")
  println(s"Calling partial function isVeryTasty = ${isVeryTasty("Glazed Donut")}")
  // NOTE: you will get scala.MatchError



  println("\nStep 4: How to define PartialFunction named isTasty and unknownTaste")
  val isTasty: PartialFunction[String, String] = {
    case "Plain Donut" => "Tasty"
  }

  val unknownTaste: PartialFunction[String, String] = {
    case donut @ _ => s"Unknown taste for donut = $donut"
  }



  println("\nStep 5: How to compose PartialFunction using orElse")
  val donutTaste = isVeryTasty orElse isTasty orElse unknownTaste
  println(donutTaste("Glazed Donut"))
  println(donutTaste("Plain Donut"))
  println(donutTaste("Chocolate Donut"))
}
