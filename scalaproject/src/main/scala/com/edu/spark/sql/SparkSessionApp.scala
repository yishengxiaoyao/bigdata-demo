package com.edu.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


object SparkSessionApp {
  def main(args: Array[String]): Unit = {
    var sparkSession = SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()

    //sparkSession.sparkContext.parallelize(Array(1,2,3)).collect().foreach(println)
    //简化
    //sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").show()
    //麻烦写法
    //val df=sparkSession.read.format("json").option("path","/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load().show()
    //sparkSession.read.json()
    //df.printSchema()
    //val df=sparkSession.table("people").show()
    //val df=sparkSession.sparkContext.parallelize(Array("1", "xiaoyao", "2", "yisheng", "3", "bruce")).toDF()
    //get the content of the page_view
    //val g5=sparkSession.table("page_view")
    //print the schema information of page_view
    //g5.printSchema()
    //添加隐士转换
    //import sparkSession.implicits._
    //get the column of the table
    //g5.select("ip").show(5)
    //g5.select($"ip").show(5)
    //g5.select('ip).show(5)
    //判断是否相等
    //g5.select($"ip"==="222.78.246.228").show(5)
    programmatically(sparkSession)
    //inferReflection(sparkSession)
    sparkSession.stop()
  }

  //解决大部分
  def programmatically(sparkSession: SparkSession): Unit = {
    //1.Create an RDD of Rows from the original RDD
    val info = sparkSession.sparkContext.textFile("/Users/renren/Downloads/page_view.dat")
    //info.take(3).foreach(println)
    //info.map(x=>{
    //  val temp=x.split("\t")
    //  (temp(6),temp(9),temp(19))
    //}).take(3).foreach(println)
    val rdd = info.map(_.split("\t")).map(x => Row(x(4), x(3), x(6).toLong))
    //2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    /*val struct=StructType(
            Array(
              StructField("ip",StringType,true),
              StructField("domain",StringType,true),
              StructField("responseSize",LongType,true)))*/
    val st = StructType(Seq(StructField("ip", StringType, true),
      StructField("domain", StringType, true),
      StructField("responseSize", LongType, true)))
    //val df=sparkSession.createDataFrame(rdd,struct)
    val other = sparkSession.createDataFrame(rdd, st)
    //df.show()
    other.show()
  }

  def inferReflection(sparkSession: SparkSession): Unit = {
    //create the rdd
    val info = sparkSession.sparkContext.textFile("/Users/renren/Downloads/page_views.dat")
    //info.take(3).foreach(println)
    //info.map(x=>{
    //  val temp=x.split("\t")
    //  (temp(6),temp(9),temp(19))
    //}).take(3).foreach(println)
    //如果这个字段不是字符串类型，需要转化为相应的类型。
    import sparkSession.implicits._
    //sparkSession.sparkContext.parallelize(Array(("1", "xiaoyao"), ("2", "yisheng"),("3", "bruce"))).toDF().show()
    val df = info.map(_.split("\t")).map(x => Info(x(4), x(3), x(6).toLong)).toDF()
    //df.show()
    // df.groupBy("domain").sum("responseSize").show()
    //变成一个临时表
    df.createOrReplaceTempView("info")
    //val infoDF=sparkSession.sql("select domain,sum(responseSize) from info group by domain")
    //在1.6可以直接操作，如果在2.0之后，需要将dataframe转换rdd在进行操作。
    //infoDF.rdd.map(x=>x(0)).collect().foreach(println)
    //infoDF.map(x=>x.getString(0)).show()
    //infoDF.rdd.map(x=>x.getAs[String](0)).collect().foreach(println)
    //需要导入
    import org.apache.spark.sql.functions._
    df.select(col("domain")).show()
    df.groupBy("domain").agg(sum("responseSize").as("rs"))
      .select("domain", "rs").show()

    //df.withColumn()
  }

  /**
    * 自定义函数
    * 1）定义函数
    * 2）注册函数
    * 3）使用函数
    */
  def parseIp(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("parseIp", (ip: String) => {
      IPUtils.getInfo(ip)
    })
  }

}

//自动将RDD转换为DataFrame
//定义表的schema
case class Info(ip: String, domain: String, repsonseSize: Long)

object IPUtils {
  def getInfo(ip: String) {
    ("shenzhen", "liantong")
  }
}

