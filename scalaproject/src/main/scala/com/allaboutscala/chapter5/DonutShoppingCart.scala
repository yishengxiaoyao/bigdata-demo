package com.allaboutscala.chapter5

class DonutShoppingCart extends DonutShoppingCartDao {
  override def add(donutName: String): Long = {
    println(s"DonutShoppingCart-> add method -> donutName: $donutName")
    1
  }

  override def update(donutName: String): Boolean = {
    println(s"DonutShoppingCart-> update method -> donutName: $donutName")
    true
  }

  override def search(donutName: String): String = {
    println(s"DonutShoppingCart-> search method -> donutName: $donutName")
    donutName
  }

  override def delete(donutName: String): Boolean = {
    println(s"DonutShoppingCart-> delete method -> donutName: $donutName")
    true
  }
}
