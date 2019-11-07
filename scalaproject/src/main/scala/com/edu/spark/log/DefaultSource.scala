package com.edu.spark.log

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider}

class DefaultSource extends DataSourceRegister with RelationProvider {

  override def shortName(): String = "log"

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation =
    new LogRelation(sqlContext, parameters("path"))
}
