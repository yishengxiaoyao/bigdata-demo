package com.allaboutscala.chapter2

object OptionApp {

  def main(args: Array[String]): Unit = {
    optionAndSome()
    optionAndNone()
    optionAndMatch()
    optionTips()
  }

  def optionAndSome(): Unit ={
    println("Step 1: How to use Option and Some - a basic example")
    val glazedDonutTaste: Option[String] = Some("Very Tasty")
    println(s"Glazed Donut taste = ${glazedDonutTaste.get}")
  }

  def optionAndNone(): Unit ={
    println("\nStep 2: How to use Option and None - a basic example")
    val glazedDonutName: Option[String] = None
    println(s"Glazed Donut name = ${glazedDonutName.getOrElse("Glazed Donut")}")
  }

  def optionAndMatch(): Unit ={
    println("\nStep 3: How to use Pattern Matching with Option")
    val glazedDonutName: Option[String] = None
    glazedDonutName match {
      case Some(name) => println(s"Received donut name = $name")
      case None       => println(s"No donut name was found!")
    }
  }

  def optionTips(): Unit ={
    println("\nTip 1: Filter None using map function")
    //使用map算子可以清除None类型的数据
    val glazedDonutTaste: Option[String] = Some("Very Tasty")
    val glazedDonutName: Option[String] = None
    glazedDonutTaste.map(taste => println(s"glazedDonutTaste = $taste"))
    glazedDonutName.map(name => println(s"glazedDonutName = $name"))
  }
}
