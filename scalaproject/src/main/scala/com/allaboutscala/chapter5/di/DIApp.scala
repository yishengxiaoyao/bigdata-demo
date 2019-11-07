package com.allaboutscala.chapter5.di

object DIApp extends App {
  println("\nStep 7: Create an instance of DonutShoppingCart and call the add, update, search and delete methods")
  val donutShoppingCart: DonutShoppingCart[String] = new DonutShoppingCart[String]()
  donutShoppingCart.add("Vanilla Donut")
  donutShoppingCart.update("Vanilla Donut")
  donutShoppingCart.search("Vanilla Donut")
  donutShoppingCart.delete("Vanilla Donut")
  println("\nStep 8: Call the checkStockQuantity method")
  donutShoppingCart.checkStockQuantity("Vanilla Donut")
}
