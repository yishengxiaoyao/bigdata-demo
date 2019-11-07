package com.book.code.practice.utils

import java.io.FileInputStream
import java.util.Properties

class ConfigParser(filename: String) {
  protected val config: Properties = loadConfig(filename)

  def getProperties: Properties = config

  def getString(key: String, defaultValue: String = ""): String = {
    config.getProperty(key, defaultValue)
  }

  def getInt(key: String, defaultValue: Int = 0): Int = {
    config.getProperty(key, defaultValue toString) toInt
  }

  def getLong(key: String, defaultValue: Long = 0): Long = {
    config.getProperty(key, defaultValue toString) toLong
  }

  def getDouble(key: String, defaultValue: Double = 0.0): Double = {
    config.getProperty(key, defaultValue toString) toDouble
  }

  def getBoolean(key: String, defaultValue: Boolean = false): Boolean = {
    config.getProperty(key, defaultValue toString) toBoolean
  }

  // 对参数进行格式化处理返回
  override def toString(): String = {
    // 迭代输出配置中的信息
    val ss = for (key <- config.keySet().toArray()) yield {
      key match {
        case e: String => s"${e}=${config.getProperty(e)}"
        case _ =>
      }
    }

    ss.mkString(", ")
  }

  private def loadConfig(filename: String): Properties = {
    val c = new Properties()
    c.load(new FileInputStream(filename))
    c
  }
}

object ConfigParser {
  private val DEFAULT_FILE = "props"

  @volatile private var instance: ConfigParser = null

  def getInstance: ConfigParser = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          instance = new ConfigParser(DEFAULT_FILE)
        }
      }
    }

    instance
  }
}