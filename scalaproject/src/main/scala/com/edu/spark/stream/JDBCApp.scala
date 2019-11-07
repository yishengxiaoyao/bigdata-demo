package com.edu.spark.stream

import kafka.common.TopicAndPartition
import scalikejdbc._
import scalikejdbc.config.DBs

object JDBCApp {
  def main(args: Array[String]) {
    DBs.setup()
    val fromOffsets = DB.readOnly { implicit session => {
      sql"select * from offsets_storage".map(rs => {
        (TopicAndPartition(rs.string("topic"), rs.int("partition")), rs.long("offset"))
      }).list().apply()
    }
    }.toMap

    fromOffsets.foreach(println)
  }

}
