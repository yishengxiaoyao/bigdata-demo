package com.edu.spark.v2

import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.StructType

trait Job extends Serializable with Logging{

  val partitions=""

  val input=""

  val format="parquet"

  def getStructType():StructType

  def etl()

}
