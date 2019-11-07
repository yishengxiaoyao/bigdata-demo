package com.edu.spark.log

import org.apache.spark.sql.DataFrameReader

object ReaderObject {

  implicit class LogDataFrameReader(val reader: DataFrameReader) extends AnyVal {
    def log(path: String) = reader.format("log").load(path)
  }

}