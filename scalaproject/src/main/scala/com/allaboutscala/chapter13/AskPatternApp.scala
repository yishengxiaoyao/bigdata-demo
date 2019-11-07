package com.allaboutscala.chapter13

import akka.actor.{Actor, ActorLogging, ActorSystem, Props}
import akka.util.Timeout

/**
  * 往actor发送消息，需要获取回复。
  */
object AskPatternApp extends App {
  println("Step 1: Create an actor system")
  val system = ActorSystem("DonutStoreActorSystem")


  println("\nStep 4: Create DonutInfoActor")
  val donutInfoActor = system.actorOf(Props[DonutInfoActor], name = "DonutInfoActor")


  println("\nStep 5: Akka Ask Pattern")

  import DonutStoreProtocol._
  import akka.pattern._

  import scala.concurrent.ExecutionContext.Implicits.global
  import scala.concurrent.duration._

  implicit val timeout = Timeout(5 second)

  val vanillaDonutFound = donutInfoActor ? Info("vanilla")
  for {
    found <- vanillaDonutFound
  } yield println(s"Vanilla donut found = $found")

  val glazedDonutFound = donutInfoActor ? Info("glazed")
  for {
    found <- glazedDonutFound
  } yield println(s"Glazed donut found = $found")

  Thread.sleep(5000)


  println("\nStep 6: Close the actor system")
  val isTerminated = system.terminate()


  println("\nStep 2: Define the message passing protocol for our DonutStoreActor")

  class DonutInfoActor extends Actor with ActorLogging {

    import AskPatternApp.DonutStoreProtocol._

    def receive = {
      case Info(name) if name == "vanilla" =>
        log.info(s"Found valid $name donut")
        sender ! true

      case Info(name) =>
        log.info(s"$name donut is not supported")
        sender ! false
    }
  }


  println("\nStep 3: Create DonutInfoActor")

  object DonutStoreProtocol {

    case class Info(name: String)

  }

}
