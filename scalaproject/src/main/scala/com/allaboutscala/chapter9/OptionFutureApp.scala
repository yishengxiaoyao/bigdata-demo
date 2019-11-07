package com.allaboutscala.chapter9

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global



object OptionFutureApp extends App{
  println("Step 1: Define a method which returns a Future Option")
  def donutStock(donut: String): Future[Option[Int]] = Future {
    // assume some long running database operation
    println("checking donut stock")
    if(donut == "vanilla donut") Some(10) else None
  }

  println("\nStep 2: Define another method which returns a Future")
  def buyDonuts(quantity: Int): Future[Boolean] = Future {
    println(s"buying $quantity donuts")
    if(quantity > 0) true else false
  }

  println("\nStep 3: Chaining Future Option using for comprehension")
  for {
    someStock  <- donutStock("vanilla donut")
    isSuccess  <- buyDonuts(someStock.getOrElse(0))
  } yield println(s"Buying vanilla donut was successful = $isSuccess")


  println("\nStep 4: Chaining Futures using flatMap")
  val buyingDonuts: Future[Boolean] = donutStock("plain donut").flatMap(qty => buyDonuts(qty.getOrElse(0)))
  val isSuccess = Await.result(buyingDonuts, 5 seconds)
  println(s"Buying vanilla donut was successful = $isSuccess")

  println(s"\nStep 5: Calling map() method over multiple futures")
  val resultFromMap: Future[Future[Boolean]] = donutStock("vanilla donut")
    .map(someQty => buyDonuts(someQty.getOrElse(0)))
  Thread.sleep(1000)
}
