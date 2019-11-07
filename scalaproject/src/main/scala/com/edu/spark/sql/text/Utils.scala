package com.edu.spark.sql.text

import org.apache.spark.sql.types.{DataType, LongType, StringType}

object Utils {
  def castTo(value: String, dataType: DataType) = {
    dataType match {
      case _: LongType => value.toLong
      case _: StringType => value
    }
  }
}
