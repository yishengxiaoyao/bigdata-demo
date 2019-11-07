package com.allaboutscala.chapter5

trait DonutShoppingCartDao {
  def add(donutName:String):Long
  def update(donutName:String):Boolean
  def search(donutName:String):String
  def delete(donutName:String):Boolean

}
