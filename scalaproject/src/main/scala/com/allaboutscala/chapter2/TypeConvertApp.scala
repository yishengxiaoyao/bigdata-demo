package com.allaboutscala.chapter2

object TypeConvertApp {
  def main(args: Array[String]): Unit = {
    //这个可以转成功，是因为short的精度没有Int精度大
    println("\nStep 3: Using Scala compiler to convert from one data type to another")
    val numberOfDonuts: Short = 1
    val minimumDonutsToBuy: Int = numberOfDonuts
    println(minimumDonutsToBuy)
  }
}
