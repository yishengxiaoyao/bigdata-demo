package com.edu.spark.high

import java.io.File

import scala.io.Source

object ImplicitApp {
  def main(args: Array[String]): Unit = {
    //隐势转换
    /*implicit def man2superman(man:Man):SuperMan=new SuperMan(man.name)
    var man=new Man("xiaoyao")
    man.fly()*/
    implicit def file2rich(file: File): RichFile = new RichFile(file)

    val file = new File("/Users/renren/Downloads/test/people.txt")
    val content = file.read()
    println(content)
  }
}

class Man(val name: String)

class SuperMan(val name: String) {
  def fly(): Unit = {
    println(s"superman $name can fly!!")
  }
}

class RichFile(val file: File) {
  def read() = Source.fromFile(file.getPath).mkString
}