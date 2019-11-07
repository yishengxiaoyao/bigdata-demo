package com.allaboutscala.chapter3

object FunctionWithVariableArguments extends App {

  println("Step 1: How to define function which takes variable number of arguments")
  def printReport(names: String*) {
    println(s"""Donut Report = ${names.mkString(" - ")}""")
  }

  println("\nStep 2: How to call function which takes variable number of String arguments")
  printReport("Glazed Donut", "Strawberry Donut", "Vanilla Donut")
  printReport("Chocolate Donut")

  println("\nStep 3: How to pass a List to a function with variable number of arguments")
  val listDonuts: List[String] = List("Glazed Donut", "Strawberry Donut", "Vanilla Donut")
  printReport(listDonuts: _*)

  println("\nStep 4: How to pass a Sequence to a function with variable number of arguments")
  val seqDonuts: Seq[String] = Seq("Chocolate Donut", "Plain Donut")
  printReport(seqDonuts: _*)

  println("\nStep 5: How to pass an Array to a function with variable number of arguments")
  val arrDonuts: Array[String] = Array("Coconut Donut")
  printReport(arrDonuts: _*)
}