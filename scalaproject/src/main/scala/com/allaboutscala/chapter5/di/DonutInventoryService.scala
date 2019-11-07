package com.allaboutscala.chapter5.di

trait DonutInventoryService[A] {

 val donutDatabase: DonutDatabase[A] // dependency injection

 def checkStockQuantity(donut: A): Int = {
  println(s"DonutInventoryService-> checkStockQuantity method -> donut: $donut")
  donutDatabase.query(donut)
  1
 }
}