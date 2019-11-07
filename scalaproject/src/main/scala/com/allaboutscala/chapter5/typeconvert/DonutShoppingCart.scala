package com.allaboutscala.chapter5.typeconvert

class DonutShoppingCart[A] extends DonutShoppingCartDao[A] with DonutInventoryService[A] {

 override def add(donut: A): Long = {
  println(s"DonutShoppingCart-> add method -> donut: $donut")
  1
 }

 override def update(donut: A): Boolean = {
  println(s"DonutShoppingCart-> update method -> donut: $donut")
  true
 }

 override def search(donut: A): A = {
  println(s"DonutShoppingCart-> search method -> donut: $donut")
  donut
 }

 override def delete(donut: A): Boolean = {
  println(s"DonutShoppingCart-> delete method -> donut: $donut")
  true
 }

 override def checkStockQuantity(donut: A): Int = {
  println(s"DonutShoppingCart-> checkStockQuantity method -> donut: $donut")
  10
 }

}