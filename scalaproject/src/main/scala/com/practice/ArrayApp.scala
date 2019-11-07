package com.practice

import com.typesafe.scalalogging.LazyLogging

/**
  * 如果object继承接口App之后，就不需要写main函数，在App中已经封装了
  */
object ArrayApp extends App with LazyLogging {
  /*def main(args: Array[String]): Unit = {
    val a=new Array[String](5)
    println(a.length)

    val b=Array("Hadoop","Spark")


  }

  def sum(nums:Int*):Int={
    if(nums.length==0){
      0
    }else{
      nums.head+sum(nums.tail:_*)
    }
  }*/
  //println("Hello World Scala!")

  //println("Command Line argument!")
  //println(args.mkString(","))


  logger.info("Hello World from Scala Logging!")

  //logger.warn("This is a warning information!")

  //logger.debug("This is a debug information!")

  println("Step 1: Using if clause as a statement")
  val numberOfPeople = 20
  val donutsPerPerson = 2

  if (numberOfPeople > 10)
    println(s"Number of donuts to buy = ${numberOfPeople * donutsPerPerson}")

  println(s"\nStep 2: Using if and else clause as a statement")
  val defaultDonutsToBuy = 8

  if (numberOfPeople > 10)
    println(s"Number of donuts to buy = ${numberOfPeople * donutsPerPerson}")
  else
    println(s"Number of donuts to buy = $defaultDonutsToBuy")

}

