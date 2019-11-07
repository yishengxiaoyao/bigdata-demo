package com.edu.spark.base

object ConstructorApp {

  def main(args: Array[String]): Unit = {
    //new 的时候就是调用构造方法
    var person = new OtherPerson("xiaoyao", 30)
    println(person.age + ":" + person.name + ":" + person.school)

    var otherperson = new OtherPerson("xiaoyao", 30, "male")
    println(otherperson.age + ":" + otherperson.name + ":" + otherperson.gender + ":" + otherperson.school)

    var student = new Student("yishengxiaoyao", 30, "bigdata")
    println(student.name + ":" + student.age + ":" + student.major + ":" + student.school)

  }

}
