package com.edu.spark.sql.text

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class TextDataSourceRelation(override val sqlContext: SQLContext,
                             path: String,
                             userSchema: StructType)
  extends BaseRelation
    with TableScan
    with PrunedScan
    with PrunedFilteredScan
    with Logging {

  override def buildScan(): RDD[Row] = {

    logWarning("this is a custome buildscan()....")
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(x => x._2)
    val schemaFields = schema.fields

    /**
      * 把field作用到rdd
      * 如何根据schema的field数据类型以及字段顺序整合到rdd
      */
    val rows = rdd.map(fileContext => {
      val lines = fileContext.split("\n")
      val data = lines.map(x => x.split(",").map(x => x.trim).toSeq)

      /**
        * 将数据要索引匹配
        */
      val typeValues = data.map(x => x.zipWithIndex.map {
        case (value, index) => {
          val colName = schemaFields(index).name
          Utils.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value == "0") {
              "male"
            } else if (value == "1") {
              "femal"
            } else {
              "weizhi"
            }
          } else {
            value
          }, schemaFields(index).dataType)
        }
      })
      typeValues.map(x => Row.fromSeq(x))
    })
    rows.flatMap(x => x)
  }

  override def schema: StructType = {
    if (userSchema != null) {
      userSchema
    } else {
      StructType(
        StructField("id", LongType, false) ::
          StructField("name", StringType, false) ::
          StructField("gender", StringType, false) ::
          StructField("salary", LongType, false) ::
          StructField("comm", LongType, false) :: Nil
      )
    }
  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {
    logWarning("this is a custome buildscan(requiredColumns)....")
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(x => x._2)
    val schemaFields = schema.fields

    /**
      * 把field作用到rdd
      * 如何根据schema的field数据类型以及字段顺序整合到rdd
      */
    val rows = rdd.map(fileContext => {
      val lines = fileContext.split("\n")
      val data = lines.map(x => x.split(",").map(x => x.trim).toSeq)

      /**
        * 将数据要索引匹配
        */
      val typeValues = data.map(x => x.zipWithIndex.map {
        case (value, index) => {
          val colName = schemaFields(index).name
          val castValue = Utils.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value == "0") {
              "male"
            } else if (value == "1") {
              "female"
            } else {
              "weizhi"
            }
          } else {
            value
          }, schemaFields(index).dataType)
          if (requiredColumns.contains(colName)) {
            Some(castValue)
          } else {
            None
          }
        }
      })
      typeValues.map(x => Row.fromSeq(x.filter(_.isDefined).map(x => x.get)))
    })
    rows.flatMap(x => x)
  }

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logWarning("this is ruozedata custom buildScan(requiredColumns,filters)...")
    logWarning("Filter:")
    filters.foreach(x => println(x.toString))
    val rdd = sqlContext.sparkContext.wholeTextFiles(path).map(x => x._2)
    val schemaFields = schema.fields

    /**
      * 把field作用到rdd
      * 如何根据schema的field数据类型以及字段顺序整合到rdd
      */

    val rows = rdd.map(fileContext => {
      val lines = fileContext.split("\n")
      val data = lines.map(x => x.split(",").map(x => x.trim).toSeq)
      /**
        * 将数据要索引匹配
        */
      val typeValues = data.map(x => x.zipWithIndex.map {
        case (value, index) => {
          val colName = schemaFields(index).name
          val dataType = schemaFields(index).dataType
          val castValue = Utils.castTo(if (colName.equalsIgnoreCase("gender")) {
            if (value == "0") {
              "male"
            } else if (value == "1") {
              "female"
            } else {
              "weizhi"
            }
          } else {
            value
          }, dataType)
          if (requiredColumns.contains(colName)) {
            Some(castValue)
          } else {
            None
          }
        }
      })
      typeValues.map(x => {
        Row.fromSeq(
          x.filter(_.isDefined)
            .map(
              x => x.get
            )
        )
      })
    })
    rows.flatMap(x => x)
  }

}
