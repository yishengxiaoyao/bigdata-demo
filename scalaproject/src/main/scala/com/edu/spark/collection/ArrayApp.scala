package com.edu.spark.collection

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object ArrayApp {

  def main(args: Array[String]): Unit = {
    //定长数组,下标从0开始
    val array = new Array[Int](5)
    //var b=Array(0,0,0,0,0)
    //长度
    println(array.length) // result 5
    //更新数组
    array(1) = 1
    for (element <- array) {
      print(element + " ")
    } //result 0 1 0 0 0 0
    println()
    array.update(2, 3)
    for (element <- array) {
      print(element + " ")
    } //result 0 1 3 0 0 0
    println()
    //最值
    println(array.max)
    println(array.min)
    //求和
    println(array.sum)
    //对数组中的数据进行拼接
    println(array.mkString(","))
    //在数组的前面和后面添加指定的符号，然后在中间使用其他符号对数据进行拼接
    println(array.mkString("[", ",", "]"))
    //ArrayBuffer 定义
    var b = ArrayBuffer[Int]()
    //添加单个元素
    b += 1
    for (element <- b) {
      print(element + " ")
    } //result 1
    println()
    //添加多个元素
    b += (2, 3, 4)
    for (element <- b) {
      print(element + " ")
    } //result 1 2 3 4
    println()
    //添加一个数组
    b ++= ArrayBuffer(5, 6, 7)
    for (element <- b) {
      print(element + " ")
    } //result 1 2 3 4 5 6 7
    println()
    b.insert(1, 8, 9)
    for (element <- b) {
      print(element + " ")
    }
    println()
    //删除单个元素
    b -= 1
    for (element <- b) {
      print(element + " ")
    } //result 2 3 4 5 6 7
    println()
    //删除某个数组或者多个元素
    b --= ArrayBuffer(5, 6, 7)
    for (element <- b) {
      print(element + " ")
    } //result 2 3 4
    println()
    //删除指定位置元素
    //b.remove(1)
    //删除指定位置在内的几个元素
    //b.remove(0,2)
    //从右边开始的几个元素删除
    //b.trimEnd(1)
    //从左边开始的几个元素删除
    //b.trimStart(1)
    //除了第一个元素之外的其他元素
    //b.tail
    //遍历
    for (element <- b) {
      print(element + " ")
    }
    println()
    println(b.indexOf(2))
    println(b.lastIndexOf(3))
    println(b.lengthCompare(6))
    println(b.last)
    println(array.last)
    println(b.size)
    println(b.mkString)
    println(array.last)
    println(array.reverse.mkString(","))
    println(b.reverse.mkString(","))
    println(array.to[Vector])
    println(b.toVector)
    array.contains(2)
    b.contains(8)
    println(array.reverse.mkString(","))
    println(b.reverse.mkString(","))
    var c = new StringBuilder("123");
    println(array.addString(c).mkString(","))
    println(array.nonEmpty)
    println(Array.concat(array, b.toArray).mkString(","))
    println(array.isEmpty)
    println(array.nonEmpty)
    println(Array.copy(array, 1, b.toArray, 1, 2))
    println(Array.range(1, 5, 2).mkString(","))
    var l = List(1, 2, 3, 4, 5)
    println(l.head)
    println(l.tail)
    println(l.last)
    println(l.lastIndexOf(4))
    var lb = ListBuffer(1, 2, 3, 4, 5)
    println(lb.toList.mkString(","))
    println(lb.toArray)
    println(lb.trimStart(1))
    println(lb.trimEnd(1))
    println(lb.tail)
    println(lb.insert(1, 11, 12))
    println(lb.length)
    //println(lb.prependToList(List(13,14))) //将某几个数加到之前的位置
    println(lb.remove(1))
    println(lb.remove(1, 2))
    println(lb.size)
    println(lb.isEmpty)
    println(lb.nonEmpty)
    println(lb.indexOf(2))
    println(lb.indexOf(2, 1))
    //map k->v bukebian
    //scala.collection.mutable.map 不可修改
    //hashmap可以修改
    //getorelse
    //map遍历
    //option
  }

  def sum(nums: Int*): Int = {
    if (nums.length == 0) {
      0
    } else {
      nums.head + sum(nums.tail: _*)
    }
  }

}
