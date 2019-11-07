package com.edu.spark.stream

import java.sql.{Connection, DriverManager}

import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.commons.pool2.{BasePooledObjectFactory, PooledObject}


object ConnectionPool {
  private val pool = new GenericObjectPool[Connection](new MysqlConnectionFactory("jdbc:mysql://103.235.245.156:3306/tutorials", "root", "root", "com.mysql.jdbc.Driver"))

  def getConnection(): Connection = {
    pool.borrowObject()
  }

  def returnConnection(conn: Connection): Unit = {
    pool.returnObject(conn)
  }
}

class MysqlConnectionFactory(url: String, userName: String, password: String, className: String) extends BasePooledObjectFactory[Connection] {
  override def create(): Connection = {
    Class.forName(className)
    DriverManager.getConnection(url, userName, password)
  }

  override def wrap(conn: Connection): PooledObject[Connection] = new DefaultPooledObject[Connection](conn)

  override def validateObject(pObj: PooledObject[Connection]) = !pObj.getObject.isClosed

  override def destroyObject(pObj: PooledObject[Connection]) = pObj.getObject.close()
}