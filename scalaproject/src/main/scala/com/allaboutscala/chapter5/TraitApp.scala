package com.allaboutscala.chapter5

object TraitApp extends App {
  println("\nStep 3: Create an instance of DonutShoppingCart and call the add, update, search and delete methods")
  val donutShoppingCart1: DonutShoppingCart = new DonutShoppingCart()
  donutShoppingCart1.add("Vanilla Donut")
  donutShoppingCart1.update("Vanilla Donut")
  donutShoppingCart1.search("Vanilla Donut")
  donutShoppingCart1.delete("Vanilla Donut")
  println("\nStep 4: Create an instance of DonutShoppingCart and assign its type to the trait DonutShoppingCartDao")
  val donutShoppingCart2: DonutShoppingCartDao = new DonutShoppingCart()
  donutShoppingCart2.add("Vanilla Donut")
  donutShoppingCart2.update("Vanilla Donut")
  donutShoppingCart2.search("Vanilla Donut")
  donutShoppingCart2.delete("Vanilla Donut")
}
