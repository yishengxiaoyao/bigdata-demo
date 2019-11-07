package com.edu.spark.base

/**
  * 在继承关系中，子类中新增的字段需要使用val或者var进行修饰。如果父类中有的属性，如果需要修改的话，需要使用override来修饰。
  *
  * @param name
  * @param age
  * @param major
  */
class Student(name: String, age: Int, var major: String) extends OtherPerson(name, age) {
  println(" enter student class")
  override val school = "USTC"
  println(" leave student class")
}


