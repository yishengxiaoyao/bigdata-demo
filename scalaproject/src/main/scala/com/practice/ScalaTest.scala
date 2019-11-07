package com.practice

object ScalaTest {

  def main(args: Array[String]): Unit = {
    /*println(10.isInstanceOf[Int])
    Array("Hadoop","Spark","Hive").foreach(println)
    Array("Hadoop","Spark","Hive").foreach(x=>println(x))
    for (i <- 0 to 10){
      if(i%2==0) {
        println(i)
      }
    }
    for (i <- 0 to 10 if (i%2 == 0)){
        println(i)
    }
    sayName("yishengxiaoyao")
    print(sum(1 to 10 :_*))*/
    //Array("Hadoop","Spark","Hive","Spark").foreach(println)
    var array = Array("Hadoop", "Spark", "Hive", "Spark")
    for (x <- array) {
      println(x)
    }

    for (x <- 1 to 10 if x % 2 == 0) {
      println(x)
    }


  }

  def sayName(name: String = "xiaoyao"): Unit = {
    println(name)
  }

  def sum(a: Int*): Int = {
    var result = 0
    for (x <- a) {
      result = result + x
    }
    result

  }

}
