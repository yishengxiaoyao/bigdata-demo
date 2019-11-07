package com.allaboutscala.chapter9

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object FoldRightApp extends App {
  println("Step 1: Define a method which returns a Future Option")
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
  println(s"\nStep 3: Call Future.reduceLeft to fold over futures results from left to right")
  val futureFoldLeft = Future.reduce(futureOperations){ case (acc, someQty) =>
    acc.map(qty => qty + someQty.getOrElse(0))
  }
  futureFoldLeft.onComplete {
    case Success(results) => println(s"Results $results")
    case Failure(e)       => println(s"Error processing future operations, error = ${e.getMessage}")
  }

}
