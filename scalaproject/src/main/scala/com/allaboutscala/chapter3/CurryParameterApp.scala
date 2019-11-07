package com.allaboutscala.chapter3

object CurryParameterApp extends App {
  println("Step 1: How to define function with curried parameter groups")
  def totalCost(donutType: String)(quantity: Int)(discount: Double): Double = {
    println(s"Calculating total cost for $quantity $donutType with ${discount * 100}% discount")
    val totalCost = 2.50 * quantity
    totalCost - (totalCost * discount)
  }



  println("\nStep 2: How to call a function with curried parameter groups")
  println(s"Total cost = ${totalCost("Glazed Donut")(10)(0.1)}")



  println("\nStep 3: How to create a partially applied function from a function with curried parameter groups")
  val totalCostForGlazedDonuts = totalCost("Glazed Donut") _



  println("\nStep 4: How to call a partially applied function")
  println(s"Total cost for Glazed Donuts ${totalCostForGlazedDonuts(10)(0.1)}")
}
