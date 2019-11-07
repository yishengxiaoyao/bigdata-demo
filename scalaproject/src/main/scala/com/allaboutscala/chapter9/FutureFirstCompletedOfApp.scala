package com.allaboutscala.chapter9

import scala.util.{Success, Failure}

object FutureFirstCompletedOfApp extends App {

  println("Step 1: Define a method which returns a Future Option")
  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.Future
  def donutStock(donut: String): Future[Option[Int]] = Future {
    println("checking donut stock")
    if(donut == "vanilla donut") Some(10) else None
  }



  println(s"\nStep 2: Create a List of future operations")
  val futureOperations = List(
    donutStock("vanilla donut"),
    donutStock("plain donut"),
    donutStock("chocolate donut"),
    donutStock("vanilla donut")
  )


  println(s"\nStep 3: Call Future.firstCompletedOf to get the results of the first future that completes")
  val futureFirstCompletedResult = Future.firstCompletedOf(futureOperations)
  futureFirstCompletedResult.onComplete {
    case Success(results) => println(s"Results $results")
    case Failure(e)       => println(s"Error processing future operations, error = ${e.getMessage}")
  }


  Thread.sleep(3000)
}
