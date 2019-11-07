package com.allaboutscala.chapter13

import akka.actor.ActorSystem

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global

object ActorSystemApp extends App {

  println("Step 1: create an actor system")
  val system = ActorSystem("DonutStoreActorSystem")


  println("\nStep 2: close the actor system")
  val isTerminated = system.terminate()


  println("\nStep 3: Check the status of the actor system")
  //注册功能
  isTerminated.onComplete {
    case Success(result) => println("Successfully terminated actor system")
    case Failure(e) => println("Failed to terminate actor system")
  }
  Thread.sleep(5000)

}
