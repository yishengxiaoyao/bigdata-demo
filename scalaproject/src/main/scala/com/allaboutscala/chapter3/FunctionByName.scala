package com.allaboutscala.chapter3

import scala.util.Random

object FunctionByName extends App {


  println("Step 1: How to define a List with Tuple3 elements")
  val listOrders = List(("Glazed Donut", 5, 2.50), ("Vanilla Donut", 10, 3.50))



  println("\nStep 2: How to define a function to loop through each Tuple3 of the List and calculate total cost")
  def placeOrder(orders: List[(String, Int, Double)])(exchangeRate: Double): Double = {
    var totalCost: Double = 0.0
    orders.foreach {order =>
      val costOfItem = order._2 * order._3 * exchangeRate
      println(s"Cost of ${order._2} ${order._1} = £$costOfItem")
      totalCost += costOfItem
    }
    totalCost
  }



  println("\nStep 3: How to call function with curried group parameter for List of Tuple3 elements")
  println(s"Total cost of order = £${placeOrder(listOrders)(0.5)}")



  println("\nStep 4: How to define a call-by-name function")
  def placeOrderWithByNameParameter(orders: List[(String, Int, Double)])(exchangeRate: => Double): Double = {
    var totalCost: Double = 0.0
    orders.foreach {order =>
      val costOfItem = order._2 * order._3 * exchangeRate
      println(s"Cost of ${order._2} ${order._1} = £$costOfItem")
      totalCost += costOfItem
    }
    totalCost
  }



  println("\nStep 5: Define a simple USD to GBP function")
  val randomExchangeRate = new Random(10)
  def usdToGbp: Double = {
    val rate = randomExchangeRate.nextDouble()
    println(s"Fetching USD to GBP exchange rate = $rate")
    rate
  }



  println("\nStep 6: How to call function with call-by-name parameter")
  println(s"Total cost of order = £${placeOrderWithByNameParameter(listOrders)(usdToGbp)}")
}
