package com.allaboutscala.chapter9


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

object FutureSequenceApp extends App {
  println("Step 1: Define a method which returns a Future Option of Int")
  def donutStock(donut: String): Future[Option[Int]] = Future {
    println("checking donut stock ... sleep for 2 seconds")
    Thread.sleep(2000)
    if(donut == "vanilla donut") Some(10) else None
  }

  println("\nStep 2: Define another method which returns a Future[Boolean]")
  def buyDonuts(quantity: Int): Future[Boolean] = Future {
    println(s"buying $quantity donuts ... sleep for 3 seconds")
    Thread.sleep(3000)
    if(quantity > 0) true else false
  }

  println("\nStep 3: Define another method for processing payments and returns a Future[Unit]")
  def processPayment(): Future[Unit] = Future {
    println("processPayment ... sleep for 1 second")
    Thread.sleep(1000)
  }

  println("\nStep 4: Combine future operations into a List")
  val futureOperation:List[Future[Any]] = List(donutStock("vanilla donut"),buyDonuts(10),processPayment())

  println(s"\nStep 5: Call Future.sequence to run the future operations in parallel")

  val futureSequenceResults = Future.sequence(futureOperation)

  futureSequenceResults.onComplete{
    case Success(results) => println(s"Results $results")
    case Failure(e) => println(s"Error processing future operations, error = ${e.getMessage}")
  }

}
