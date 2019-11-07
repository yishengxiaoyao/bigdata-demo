package com.allaboutscala.chapter13

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}

/**
  * 发送数据到actor，是否有回复无所谓。
  */
object TellPatternApp extends App {
  println("Step 1: Create an actor system")
  val system = ActorSystem("DonutStoreActorSystem")

  /**
    * 创建Actor
    */
  println("\nStep 4: Create DonutInfoActor")
  val donutInfoActor = system.actorOf(Props[DonutInfoActor], name = "DonutInfoActor")


  println("\nStep 5: Akka Tell Pattern")

  import DonutStoreProtocol._

  donutInfoActor ! Info("vanilla")


  println("\nStep 6: close the actor system")
  val isTerminated = system.terminate()


  println("\nStep 2: Define the message passing protocol for our DonutStoreActor")

  class DonutInfoActor extends Actor with ActorLogging {

    import TellPatternApp.DonutStoreProtocol._

    /**
      *
      * @return
      */
    def receive = {
      case Info(name) =>
        log.info(s"Found $name donut")
    }
  }


  println("\nStep 3: Define DonutInfoActor")

  object DonutStoreProtocol {

    case class Info(name: String)

  }

}
