package com.practice

//单例
object Timer {

  var index = 0

  def main(args: Array[String]): Unit = {
    for (i <- 1 to 10) {
      println(incr())
    }
  }

  def incr() = {
    index = index + 1
    index
  }
}
