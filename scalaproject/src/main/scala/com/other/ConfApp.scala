package com.other

import com.typesafe.config.ConfigFactory

object ConfApp {
  def main(args: Array[String]): Unit = {
    val config = ConfigFactory.load()
    val user = config.getString("db.default.url")
    println(user)
    println(None.toString)
  }
}
