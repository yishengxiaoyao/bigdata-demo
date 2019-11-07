package com.edu.spark.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object DayJob extends Job with Logging{
  override def getStructType(): StructType = {
    StructType(StructField("Long",LongType,false)::Nil)
  }

  override def etl(): Unit = {
    //1.
    //2.
    //3.
  }
}
