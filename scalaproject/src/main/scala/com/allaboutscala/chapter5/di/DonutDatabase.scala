package com.allaboutscala.chapter5.di

trait DonutDatabase[A] {

 def addOrUpdate(donut: A): Long

 def query(donut: A): A

 def delete(donut: A): Boolean
}