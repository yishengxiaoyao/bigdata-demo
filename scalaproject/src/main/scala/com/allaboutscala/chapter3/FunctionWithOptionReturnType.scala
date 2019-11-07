package com.allaboutscala.chapter3

object FunctionWithOptionReturnType extends App {


  println(s"Step 1: Define a function which returns an Option of type String")
  def dailyCouponCode(): Option[String] = {
    // look up in database if we will provide our customers with a coupon today
    val couponFromDb = "COUPON_1234"
    Option(couponFromDb).filter(_.nonEmpty)
  }



  println(s"\nStep 2: Call function with Option return type using getOrElse")
  val todayCoupon: Option[String] = dailyCouponCode()
  println(s"Today's coupon code = ${todayCoupon.getOrElse("No Coupon's Today")}")



  println(s"\nStep 3: Call function with Option return type using pattern matching")
  dailyCouponCode() match {
    case Some(couponCode) => println(s"Today's coupon code = $couponCode")
    case None => println(s"Sorry there is no coupon code today!")
  }



  println(s"\nStep 4: Call function with Option return type using map")
  dailyCouponCode().map(couponCode => println(s"Today's coupon code = $couponCode"))



  println("\nStep 5: Review function from previous tutorial which has an Option parameter")
  def calculateDonutCost(donutName: String, quantity: Int, couponCode: Option[String]): Double = {
    println(s"Calculating cost for $donutName, quantity = $quantity")

    couponCode match {
      case Some(coupon) =>
        val discount = 0.1 // Let's simulate a 10% discount
      val totalCost = 2.50 * quantity * (1 - discount)
        totalCost

      case None => 2.50 * quantity
    }
  }

  // apply daily coupon code if we have one
  println(s"""Total cost with daily coupon code = ${calculateDonutCost("Glazed Donut", 5, dailyCouponCode())}""")



  println(s"\nTip - 1: Call function with Option return type using fold")
  val todayCouponUsingFold: String = dailyCouponCode().fold("No Coupon Available")(couponCode => couponCode)
  println(s"Today's coupon code = $todayCouponUsingFold")
}
