package com.allaboutscala.chapter9

import scala.util.{Failure, Success}
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object FutureTraverseApp extends App {
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

  println(s"\nStep 3: Call Future.traverse to convert all Option of Int into Int")
  /**
    * 将方法进行遍历，并将结果变成想成的类型，添加到list中
    */
  val futureTraverseResult = Future.traverse(futureOperations){ futureSomeQty =>
    futureSomeQty.map(someQty => someQty.getOrElse(0))
  }
  futureTraverseResult.onComplete {
    case Success(results) => println(s"Results $results")
    case Failure(e)       => println(s"Error processing future operations, error = ${e.getMessage}")
  }


  println(s"\nStep 4: Call Future.foldLeft to fold over futures results from left to right")
  /**
    * 先遍历获取相应的数据，然后执行foldLeft/fold操作。
    */
    Future
  val futureFoldLeft = Future.fold(futureOperations)(0){
    case (acc,someQty) =>
      acc+someQty.getOrElse(0)
  }

  futureFoldLeft.onComplete {
    case Success(results) => println(s"Results $results")
    case Failure(e) => println(s"Error processing future operations, error = ${e.getMessage}")
  }

}
