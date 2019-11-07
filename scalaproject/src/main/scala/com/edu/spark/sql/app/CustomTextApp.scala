package com.edu.spark.sql.app

import org.apache.spark.sql.SparkSession

object CustomTextApp {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("CustomTextApp")
      .master("local[2]")
      .getOrCreate()

    val people = sparkSession.read.format("com.edu.spark.sql.text").option("path", "/Users/renren/Downloads/test").load()
    //people.select("id","name","salary").filter('id>10000).filter('salary>20000).show()
    //people.select("id","name","salary").write
    // .format("text").option("header","true").option("inferSchema", "true")
    // .option("delimiter", ",").save("/Users/renren/Downloads/test/other.txt")
    //people.show()
    people.select("id", "name", "salary").write.format("com.edu.spark.sql.text").save()
    sparkSession.stop()
  }
}
