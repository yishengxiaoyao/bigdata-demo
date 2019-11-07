package com.practice

object LeetCodeApp extends App{
  var str:String = "Hello"
  /*if (str.isEmpty){
    println(str)
  }*/

  println(str.map(x=>{
    if (x>='A'&&x<='Z'){
      (x+32).asInstanceOf[Char]
    }else{
      x
    }
  }))
}
