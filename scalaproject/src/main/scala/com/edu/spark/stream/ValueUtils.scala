package com.edu.spark.stream

import com.typesafe.config.ConfigFactory


object ValueUtils {
  /**
    * 加载默认的configuration,配置文件的名称为application.conf
    */
  val load = ConfigFactory.load()

  def main(args: Array[String]) {
    println(getStringValue("metadata.broker.list"))
  }

  def getStringValue(key: String, defaultValue: String = "") = {
    load.getString(key)
  }

}
