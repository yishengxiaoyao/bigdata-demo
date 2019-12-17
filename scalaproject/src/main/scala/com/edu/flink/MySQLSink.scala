package com.edu.flink

import java.sql.{Connection, PreparedStatement}

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

class MySQLSink extends RichSinkFunction[Student]{
  var connection:Connection = _
  var pstsm:PreparedStatement = _


  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    super.invoke(value,context)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }

}
