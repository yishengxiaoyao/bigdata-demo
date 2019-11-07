package com.edu.spark.high

object StringApp {
  def main(args: Array[String]): Unit = {
    var name = "xiaoyao"
    println("Hello " + name)
    println(s"hello:$name") //s表示结果为字符串，后面的可以使用 占位符($变量名)
    //原样输出
    var b =
    """
      |Welcome to Beijing.
      |Please visit the site
      |http://www.baidu.com
    """.stripMargin
    println(b)
  }
}
