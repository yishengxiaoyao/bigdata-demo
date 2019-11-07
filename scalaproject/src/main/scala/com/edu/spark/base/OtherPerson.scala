package com.edu.spark.base

//主构造器
class OtherPerson(var name: String, var age: Int) extends Serializable {
  println(" enter otherperson class")
  val school = "CNU"
  var gender = "male"
  println(" leave otherperson class")

  //附属构造器：第一行必须要调用自己的主构造器，或者其他附属构造器
  def this(name: String, age: Int, gender: String) {
    this(name, age)
    this.gender = gender
  }
}
