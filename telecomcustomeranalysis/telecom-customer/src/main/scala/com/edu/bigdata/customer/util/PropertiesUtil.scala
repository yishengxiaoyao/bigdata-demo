package com.edu.bigdata.customer.util

import java.io.InputStreamReader
import java.util.Properties

import com.typesafe.config.ConfigFactory

object PropertiesUtil {

  //第一种配置方式

  /*val config = ConfigFactory.load("kafka.properties")

  def getProperty(key: String): String = {
    config.getString(key)
  }

  def getInt(key: String): Int = {
    config.getInt(key)
  }

  def getBoolean(key: String): Boolean = {
    config.getBoolean(key)
  }

  def getDouble(key: String): Double = {
    config.getDouble(key)
  }

  def getLong(key: String): Long = {
    config.getLong(key)
  }*/

  def getProperties(path:String):Properties={
    val properties:Properties = new Properties()
    properties.load(PropertiesUtil.getClass.getClassLoader.getResourceAsStream(path))
    properties
  }

  val properties:Properties = getProperties("kafka.properties")

  def getProperty(key:String): String ={
      properties.getProperty(key)
  }

  def main(args: Array[String]): Unit = {
    getProperty("bootstrap.servers")
  }

}
