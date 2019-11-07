package com.allaboutscala.chapter5.di

class CassandraDonutStore[A] extends DonutDatabase[A] {

 override def addOrUpdate(donut: A): Long = {
  println(s"CassandraDonutDatabase-> addOrUpdate method -> donut: $donut")
  1
 }

 override def query(donut: A): A = {
  println(s"CassandraDonutDatabase-> query method -> donut: $donut")
  donut
 }

 override def delete(donut: A): Boolean = {
  println(s"CassandraDonutDatabase-> delete method -> donut: $donut")
  true
 }
}