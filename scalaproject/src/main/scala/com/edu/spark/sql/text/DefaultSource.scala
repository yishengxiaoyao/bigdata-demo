package com.edu.spark.sql.text

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}


class DefaultSource extends RelationProvider with SchemaRelationProvider with DataSourceRegister with CreatableRelationProvider {

  override def shortName(): String = "udftext"

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, null)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    //读取文件
    val path = parameters.get("path")
    path match {
      case Some(p) => new TextDataSourceRelation(sqlContext, p, schema)
      case _ => throw new IllegalArgumentException("Path is required for custom-text-data-source-api")
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val pathParameter = parameters.getOrElse("path", "./output/")
    val fsPath = new Path(pathParameter)
    val fs = fsPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    mode match {
      case SaveMode.Append => sys.error("Append mode is not supported by " + this.getClass.getCanonicalName); sys.exit(1)
      case SaveMode.Overwrite => fs.delete(fsPath, true)
      case SaveMode.ErrorIfExists if fs.exists(fsPath) => sys.error("Given path: " + pathParameter + " already exists!!"); sys.exit(1)
      case SaveMode.ErrorIfExists => sys.error("Given path: " + pathParameter + " already exists!!"); sys.exit(1)
      case SaveMode.Ignore => sys.exit()
    }

    val formatName = parameters.getOrElse("format", "customFormat")
    formatName match {
      case "customFormat" => saveAsCustomFormat(data, pathParameter, mode)
      case _ => throw new IllegalArgumentException(formatName + " is not supported!")
    }
    createRelation(sqlContext, parameters, data.schema)
  }

  private def saveAsCustomFormat(data: DataFrame, path: String, mode: SaveMode): Unit = {
    val customFormatRDD = data.rdd.map(row => {
      row.toSeq.map(value => value.toString).mkString(";")
    })
    customFormatRDD.saveAsTextFile(path)
  }


}
