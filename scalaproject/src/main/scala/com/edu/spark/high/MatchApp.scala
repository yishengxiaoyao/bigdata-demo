package com.edu.spark.high

object MatchApp {
  def main(args: Array[String]): Unit = {
    /*val peoples=Array("xiaoyao","yishengxiaoyao","xiaoyaoyisheng","xiaoyaoshan")
    val people=peoples(Random.nextInt(peoples.length))
    people match {
      case "xiaoyao" => println("xiaoyao")
      case "yishengxiaoyao" => println("yishengxiaoyao")
      case "xiaoyaoyisheng" => println("xiaoyaoyisheng")
      case "xiaoyaoshan" => println("xiaoyaoshan")
        //没有匹配到的选项
      case _ => println("no match")
    }
    greeting(Array("zhangsan")) //Hello zhangsan!
    greeting(Array("li","wangwu")) //Hello li,wangwu!
    greeting(Array("zhangsan","li","wangwu")) //Hello zhangsan and others!
    greeting(Array("li","wangwu","zhangsan")) //Welcome!*/
    /*try{
      val i=1/0
    }catch {
      case e:ArithmeticException => println("除数不能为0!")
      case e:Exception => println("捕获到异常!")
    }finally {
      println("the finally module")
    }*/
    greetMan("xiaoyao")
    println(greetManPartial("xiaoyao"))
  }

  def greetMan(name: String) = name match {
    case "zhangsan" => println("Hello zhangsan!")
    case "xiaoyao" => println("Hello xiaoyao!")
    case _ => println("Welcome!")
  }

  /**
    * 花括号内没有match的一组case就是一个偏函数
    * 第一参数为输入值的类型，第二个是输出值的类型
    */
  def greetManPartial: PartialFunction[String, String] = {
    case "zhangsan" => "Hello zhangsan!"
    case "xiaoyao" => "Hello xiaoyao!"
    case _ => "Welcome!"
  }

  def greeting(array: Array[String]): Unit = {
    array match {
      case Array("zhangsan") => println("Hello zhangsan!")
      case Array(x, y) => println(s"Hello $x,$y!")
      case Array("zhangsan", _*) => println("Hello zhangsan and others!")
      case _ => println("Welcome!")
    }
  }
}
