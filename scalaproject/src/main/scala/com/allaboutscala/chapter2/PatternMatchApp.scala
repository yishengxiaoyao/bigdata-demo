package com.allaboutscala.chapter2

object PatternMatchApp {

  def main(args: Array[String]): Unit = {
    matchNoReturn()
    matchWithReturn()
    matchDefault()
    matchMoreItems()
    matchIfExpression()
    matchType()
  }

  def matchNoReturn(): Unit ={
    println("Step 1: Pattern matching 101 - a very basic example")
    val donutType = "Glazed Donut"
    donutType match {
      /**
        * 只执行一个case
        */
      case "Glazed Donut" => println("Very tasty")
      case "Plain Donut" => println("Tasty")
    }
  }

  def matchWithReturn(){
    println("\nStep 2: Pattern matching and return the result")
    val donutType = "Glazed Donut"
    val tasteLevel = donutType match {
      case "Glazed Donut" => "Very tasty"
      case "Plain Donut" => "Tasty"
    }
    println(s"Taste level of $donutType = $tasteLevel")
  }

  def matchDefault(): Unit ={
    println("\nStep 3: Pattern matching using the wildcard operator")
    val donutType = "Glazed Donut"
    val tasteLevel2 = donutType match {
      case "Glazed Donut" => "Very tasty"
      case "Plain Donut" => "Tasty"
      /**
        * 默认值
        */
      case _ => "Tasty"
    }
    println(s"Taste level of $donutType = $tasteLevel2")
  }

  def matchMoreItems(): Unit ={
    println("\nStep 4: Pattern matching with two or more items on the same condition")
    val donutType = "Glazed Donut"
    val tasteLevel3 = donutType match {
      /**
        * 如果是或的关系，可以使用|
        * 如果是与的关系，可以使用&
        */
      case "Glazed Donut" | "Strawberry Donut" => "Very tasty"
      case "Plain Donut" => "Tasty"
      case _ => "Tasty"
    }
    println(s"Taste level of $donutType = $tasteLevel3")
  }

  def matchIfExpression(): Unit ={
    println("\nStep 5; Pattern matching and using if expressions in the case clause")
    val donutType = "Glazed Donut"
    val tasteLevel4 = donutType match {
      /**
        * 使用一个变量，来进行一些操作
        */
      case donut if (donut.contains("Glazed") || donut.contains("Strawberry")) => "Very tasty"
      case "Plain Donut"  => "Tasty"
      case _  => "Tasty"
    }
    println(s"Taste level of $donutType = $tasteLevel4")
  }

  def matchType(): Unit ={
    println("\nStep 6: Pattern matching by types")
    val priceOfDonut: Any = 2.50
    val priceType = priceOfDonut match {
      /**
        * 根据推导类型来进行匹配
        */
      case price: Int => "Int"
      case price: Double => "Double"
      case price: Float => "Float"
      case price: String => "String"
      case price: Boolean => "Boolean"
      case price: Char => "Char"
      case price: Long => "Long"
    }
    println(s"Donut price type = $priceType")
  }

}
