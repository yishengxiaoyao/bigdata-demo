package com.practice

object ConstructorApp {
  def main(args: Array[String]): Unit = {
    //new 调用构造方法
    /*val person= new Person("xiayao",10)
    print(person.school)

    val person2= new Person("yishengxiaoyao",11,"Male")

    System.out.println(person2.gender)*/
    val student = new Student("xiaoyao", 20, "Information Management")
    println(student.name + " " + student.age + " " + student.major + " " + student.school)

    println(student)

  }
}

//主构造器
class Person(val name: String, val age: Int) {
  println("===person enter====")
  val school = "ustc"
  var gender: String = _
  println("===person leave====")

  /**
    * 附属构造器 def this
    * 第一行必须使用主构造器或者其他附属构造器
    *
    */
  def this(name: String, age: Int, gender: String) {
    this(name, age)
    this.gender = gender
  }
}

class Student(name: String, age: Int, val major: String) extends Person(name, age) {
  println("==student enter===")
  override val school: String = "lanxiangjixiao"
  println("==student leave===")

  override def toString: String = "student tostring"
}