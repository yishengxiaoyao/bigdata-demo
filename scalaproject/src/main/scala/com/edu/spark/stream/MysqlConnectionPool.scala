package com.edu.spark.stream

import java.sql.Connection

import org.apache.commons.pool2.impl.GenericObjectPool

object MysqlConnectionPool {
  def apply(url: String, userName: String, password: String, className: String): GenericObjectPool[Connection] = {
    new GenericObjectPool[Connection](new MysqlConnectionFactory(url, userName, password, className))
  }
}
