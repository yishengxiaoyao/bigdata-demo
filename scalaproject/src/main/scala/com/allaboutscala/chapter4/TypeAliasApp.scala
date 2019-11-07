package com.allaboutscala.chapter4

object TypeAliasApp extends App {


  println("Step 1: How to define a case class to represent a Donut object")
  case class Donut(name: String, price: Double, productCode: Option[Long] = None)



  println("\nStep 2: How to create instances or objects for the Donut case class")
  val vanillaDonut: Donut = Donut("Vanilla", 1.50)
  val glazedDonut: Donut = Donut("Glazed", 2.0)
  println(s"Vanilla Donut = $vanillaDonut")
  println(s"Glazed Donut = $glazedDonut")



  println("\nStep 3: How to use type alias to name a Tuple2 pair into a domain type called CartItem")
  type CartItem[Donut, Int] = Tuple2[Donut, Int]



  println("\nStep 4: How to create instances of the aliased typed CartItem")
  val cartItem = new CartItem(vanillaDonut, 4)
  println(s"cartItem = $cartItem")
  println(s"cartItem first value = ${cartItem._1}")
  println(s"cartItem second value = ${cartItem._2}")



  println("\nStep 5: How to use an aliased typed into a function parameter")
  def calculateTotal(shoppingCartItems: Seq[CartItem[Donut, Int]]): Double = {
    // calculate the total cost
    shoppingCartItems.foreach { cartItem =>
      println(s"CartItem donut = ${cartItem._1}, quantity = ${cartItem._2}")
    }
    10 // some random total cost
  }



  println("\nStep 6: How to use a case class instead of an aliased typed")
  case class ShoppingCartItem(donut: Donut, quantity: Int)

  val shoppingItem: ShoppingCartItem = ShoppingCartItem(Donut("Glazed Donut", 2.50), 10)
  println(s"shoppingItem donut = ${shoppingItem.donut}")
  println(s"shoppingItem quantity = ${shoppingItem.quantity}")



  println("\nStep 7: How to use case class from Step 6 to represent a Sequence of Donut items in a shopping cart")
  def calculateTotal2(shoppingCartItems: Seq[ShoppingCartItem]): Double = {
    // calculate the total cost
    shoppingCartItems.foreach { shoppingCartItem =>
      println(s"ShoppingCartItem donut = ${shoppingCartItem.donut}, quantity = ${shoppingCartItem.quantity}")
    }
    10 // some random total cost
  }
}
