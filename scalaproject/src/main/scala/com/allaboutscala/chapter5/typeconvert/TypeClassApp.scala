package com.allaboutscala.chapter5.typeconvert

object TypeClassApp extends App {
  println("\nStep 3: Create an instance of DonutShoppingCart of type String and call the add, update, search and delete methods")
  val donutShoppingCart1: DonutShoppingCart[String] = new DonutShoppingCart[String]()
  donutShoppingCart1.add("Vanilla Donut")
  donutShoppingCart1.update("Vanilla Donut")
  donutShoppingCart1.search("Vanilla Donut")
  donutShoppingCart1.delete("Vanilla Donut")
  println("\nStep 5: Call the checkStockQuantity method which was inherited from trait DonutInventoryService")
  donutShoppingCart1.checkStockQuantity("Vanilla Donut")
  /**
    * 如果某个类继承多个trait或者抽象类，不能使用下面的方法
    */
  /*println("\nStep 4: Create an instance of DonutShoppingCart of type String and assign its type to the trait DonutShoppingCartDao")
  val donutShoppingCart2: DonutShoppingCartDao[String] = new DonutShoppingCart[String]()
  donutShoppingCart2.add("Vanilla Donut")
  donutShoppingCart2.update("Vanilla Donut")
  donutShoppingCart2.search("Vanilla Donut")
  donutShoppingCart2.delete("Vanilla Donut")*/

}
