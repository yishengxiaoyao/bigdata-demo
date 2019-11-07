package com.edu.spark.file

import scala.io.Source


object FileApp {
  def main(args: Array[String]): Unit = {
    //读取文件
    val file = Source.fromFile("/Users/Downloads/test/people.txt")
    for (line <- file.getLines()) {
      println(line)
    }
    //指定url
    Source.fromURL("")
  }
}
