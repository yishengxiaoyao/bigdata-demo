package com.allaboutscala.chapter2

object TypeApp {
  def main(args: Array[String]): Unit = {
    typeAny()
    typeAnyRef()
    typeAnyVal()
  }
  def typeAny(): Unit ={
    println("Step 1: Declare a variable of type Any")
    val favoriteDonut: Any = "Glazed Donut"
    println(s"favoriteDonut of type Any = $favoriteDonut")
  }

  def typeAnyRef(): Unit ={
    println("\nStep 2: Declare a variable of type AnyRef")
    val donutName: AnyRef = "Glazed Donut"
    println(s"donutName of type AnyRef = $donutName")
  }

  def typeAnyVal(): Unit ={
    println("\nStep 3: Declare a variable of type AnyVal")
    val donutPrice: AnyVal = 2.50
    println(s"donutPrice of type AnyVal = $donutPrice")
  }
}
