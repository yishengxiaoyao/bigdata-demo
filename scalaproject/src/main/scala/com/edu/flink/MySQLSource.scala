package com.edu.flink

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MySQLSource extends RichSourceFunction[Student]{
  var connection:Connection = _
  var pstmt:PreparedStatement = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    pstmt = connection.prepareStatement("")
  }

  override def close(): Unit = {
    super.close()
  }

  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val resultSet = pstmt.executeQuery()
    while(resultSet.next()){
      val student = Student(1,"xiaoyao",12)


    }
  }

  override def cancel(): Unit = {

  }

}
