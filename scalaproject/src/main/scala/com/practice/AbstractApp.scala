package com.practice

object AbstractApp {
  def main(args: Array[String]): Unit = {
    val s = new Student1()
    s.hitPerson
    println(s.name)
  }
}

abstract class Person1 {
  val name: String

  def hitPerson
}

class Student1 extends Person1 {
  override val name: String = "doudou"

  override def hitPerson: Unit = {
    println("hit doudou")
  }
}