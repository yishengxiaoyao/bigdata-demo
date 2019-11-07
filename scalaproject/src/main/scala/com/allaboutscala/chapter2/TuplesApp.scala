package com.allaboutscala.chapter2

object TuplesApp {
  def main(args: Array[String]): Unit = {
    tuple2Store()
    tuple2StoreForEach()
    tuple3Store()
    tupleShort()
    tupleMatch()
    tupletips()
  }

  def tuple2Store(): Unit = {
    println("Step 1: Using Tuple2 to store 2 data points")
    val glazedDonutTuple = Tuple2("Glazed Donut", "Very Tasty")
    println(s"Glazed Donut with 2 data points = $glazedDonutTuple")
  }

  def tuple2StoreForEach(): Unit = {
    println("\nStep 2: Access each element in tuple")
    val glazedDonutTuple = Tuple2("Glazed Donut", "Very Tasty")
    val donutType = glazedDonutTuple._1
    val donutTasteLevel = glazedDonutTuple._2
    println(s"$donutType taste level is $donutTasteLevel")
  }

  def tupleShort(): Unit = {
    println("\nStep 5: Shortcut for creating Tuples")
    val chocolateDonut = ("Chocolate Donut", "Very Tasty", 3.0)
    println(s"Chocolate donut taste level = ${chocolateDonut._2}, price = ${chocolateDonut._3}")
  }

  def tuple3Store(): Unit = {
    println("\nStep 3: Using TupleN classes to store more than 2 data points")
    val glazedDonut = Tuple3("Glazed Donut", "Very Tasty", 2.50)
    println(s"${glazedDonut._1} taste level is ${glazedDonut._2} and it's price is ${glazedDonut._3}")
  }

  def tupleMatch(): Unit = {
    println("\nStep 4: How to use pattern matching on Tuples")
    val strawberryDonut = Tuple3("Strawberry Donut", "Very Tasty", 2.50)
    val plainDonut = Tuple3("Plain Donut", "Tasty", 2)
    val glazedDonut = Tuple3("Glazed Donut", "Very Tasty", 2.50)
    val donutList = List(glazedDonut, strawberryDonut, plainDonut)
    val priceOfPlainDonut = donutList.foreach( tuple => {
      tuple match {
        case ("Plain Donut", taste, price) => println(s"Donut type = Plain Donut, taste = $taste ,price = $price")
        case d if d._1 == "Glazed Donut" => println(s"Donut type = ${d._1}, price = ${d._3}")
        case _ => None
      }
    })
  }

  def tupletips(): Unit ={
    println("\nStep 5 Tips: How to use pattern matching on Tuples")
    val strawberryDonut = Tuple3("Strawberry Donut", "Very Tasty", 2.50)
    val plainDonut = Tuple3("Plain Donut", "Tasty", 2)
    val glazedDonut = Tuple3("Glazed Donut", "Very Tasty", 2.50)
    val donutList = List(glazedDonut, strawberryDonut, plainDonut)
    val priceOfPlainDonut = donutList.foreach{
        case ("Plain Donut", taste, price) => println(s"Donut type = Plain Donut, taste = $taste ,price = $price")
        case d if d._1 == "Glazed Donut" => println(s"Donut type = ${d._1}, price = ${d._3}")
        case _ => None
      }
  }
}
