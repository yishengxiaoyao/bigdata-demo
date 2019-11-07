package com.edu.spark.base

object FunctionApp {
  def main(args: Array[String]): Unit = {
    //println(add2(3,5))
    //sayHello
    //sayHello()
    //Array("Hadoop","Spark","Hive","Flink").foreach(x=>println(x))
    /*for(x<-Array("Hadoop","Spark","Hive","Flink")){
      println(x)
    }*/
    //或者其他的遍历
    /* for(i<- 1 to 10 if(i%2 == 0)){
       println(i)
     }*/
    //默认参数
    //sayName()
    //println(speed(100,5))
    //println(speed(distance = 100,time = 5))
    println(sum(3, 4))
    println(sum(3, 4, 5))
    println(sum(1 to 10: _*))
  }

  def sum(argv: Int*) = {
    var result = 0
    for (element <- argv) {
      result += element
    }
    result
  }

  //其他简单写法如下所示

  def add(x: Int, y: Int): Int = {
    x + y
  }

  def add1(x: Int, y: Int) = {
    x + y
  }

  def add2(x: Int, y: Int) = x + y

  def sayHello(): Unit = {
    print("say hello")
  }

  def sayName(name: String = "xiaoyao"): Unit = {
    println(name)
  }

  def speed(distance: Float, time: Float) = {
    distance / time
  }
}
