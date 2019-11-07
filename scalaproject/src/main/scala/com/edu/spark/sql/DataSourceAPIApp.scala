package com.edu.spark.sql

import java.io.File
import java.util.Properties

import org.apache.spark.sql.functions.{sum, _}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class People(name: String, age: Long)

object DataSourceAPIApp {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("DataSourceAPIApp")
      .master("local[2]")
      .getOrCreate()

    //readData(sparkSession)
    //inferReflection(sparkSession)
    //programmatically(sparkSession)
    udfOperation(sparkSession)
    //udfTypeOperation(sparkSession)
    //parquetFileOperation(sparkSession)
    //hiveTableOperation()
    //jdbcDataSource()
    //otherTest(sparkSession)
    //sparkSession.stop()
  }

  def udfOperation(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("myaverage", UDFAgg)
    val df = sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/employees.json")
    df.createOrReplaceTempView("employees")
    //df.show()*/
    val result = sparkSession.sql("SELECT myAverage(salary) as average_salary FROM employees").explain()
    //result.show()
  }

  def readData(sparkSession: SparkSession): Unit = {
    //read 读取非流数据,指定格式,加载地址可以是具体文件，或者文件夹，或者多个文件地址
    //sparkSession.read.format("text").load("").show(false)
    //sparkSession.read.text().show()
    //val df=sparkSession.read.format("json").option("path","/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
    //val df=sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")
    //加载默认的格式为parquet。
    //val df=sparkSession.read.load("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet")
    //println(sparkSession.conf.get("spark.sql.sources.default")) //输出为parquet
    //readStream 读取流式数据
    //sparkSession.readStream
    //sparkSession.read.format("csv").option("header",true).load("")
    //写入数据, 数据写入文件只能写到文件夹，不能指定文件名
    //val df=sparkSession.read.load("")
    //df.select("name").write.format("").mode("overwrite").save()
    //df.createOrReplaceTempView("people")
    //df.select("name").show()
    //df.printSchema()
    //import sparkSession.implicits._
    //df.select($"name").show()
    //df.select('name,'age+1).show()
    //df.filter($"age">20).show()
    //df.filter('age>20).show()
    //df.filter("age>20").show()
    //df.filter('age===19).show()
    //df.filter($"age"===19).show()
    //df.filter("age=19").show()
    //df.filter("age==19").show()
    //df.createOrReplaceGlobalTempView("people")
    //df.select("name").write.format("text").save("/Users/renren/Downloads/test/name.txt")
    //sparkSession.sql("SELECT * FROM global_temp.people").show()
    //val df=sparkSession.read.format("json").option("path","/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
    //df.select("name").write.format("json").mode(SaveMode.Overwrite).save("/Users/renren/Downloads/test/")
    //val df=sparkSession.read.format("json").option("path","/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
    //df.select("name").write.saveAsTable("people")
    //sparkSession.sql("select * from people").show()
    //sparkSession.table("people").show()
    //df.rdd.saveAsTextFile("/Users/renren/Downloads/test/people.txt")
    //sparkSession.sql("select * from parquet.`/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet`").show()
    //df.write.format("text").mode(SaveMode.Overwrite).save("/Users/renren/Downloads/test/")
    import sparkSession.implicits._
    //读取数据，直接对应到类
    //val peolpleDs = sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").as[People]
    //peolpleDs.show()
    //将数据直接序列化
    val ds = Seq(People("Andy", 32)).toDS()
    ds.show()

  }

  def inferReflection(sparkSession: SparkSession): Unit = {
    //get the origin data
    val info = sparkSession.sparkContext.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    //split the data and convert to an object
    import sparkSession.implicits._
    //简单写法
    val df = info.map(_.split(",")).map(x => People(x(0), x(1).toLong)).toDF()
    //复杂写法,在使用这种方式时，需要表明每一列的名称
    //val df=info.map(x=>{
    //  val temp=x.split(",")
    //  (temp(0),temp(1).toLong)
    //}).toDF("name","age")
    //Before Spark2.0,we can operate directly.Since Spark2.0,we need to convert the dataframe to rdd.
    //df.map(x=>x(0)).collect().foreach(println) //before spark2.0
    //df.rdd.map(x=>x(0)).collect().foreach(println) //since spark2.0
    //df.rdd.map(x=>x.get(0)).collect().foreach(println)
    //df.rdd.map(x=>x.getAs[String](0)).collect().foreach(println)
    //df.rdd.map(x=>x.getString(0)).collect().foreach(println)
    //df.rdd.map(x=>x.getAs[String]("name")).collect().foreach(println)
    //df.select(col("name")).show()
    //df.groupBy("name").agg(sum("age").as("age"))
    // .select("name","age").show()
    //df.groupBy("name").agg(Map("age"->"max")).show()
  }

  def programmatically(sparkSession: SparkSession): Unit = {
    /**
      * 1.Create an RDD of Rows from the original RDD;
      * 2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
      * 3.Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
      */
    //1.Create an RDD of Rows from the original RDD;
    val info = sparkSession.sparkContext.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    val rdd = info.map(_.split(",")).map(x => Row(x(0), x(1).toLong))
    val struct = StructType(
      StructField("name", StringType, false) ::
        StructField("age", LongType, false) :: Nil)
    //第二种写法
    /*val struct= StructType(
                Array(StructField("name", StringType, false),
                  StructField("age", LongType, false)))*/
    val df = sparkSession.createDataFrame(rdd, struct)
    //使用sql的方式
    /*sparkSession.udf.register("strlen",(name:String,age:Long)=>{
      getStringLength(name,age)
    })
    df.registerTempTable("people")
    df.createOrReplaceTempView("people")
    sparkSession.sql("select name,strlen(name) as length from people").show()*/
    //udf第二种方式
    /*val code=(args0:String)=>{
      (args0.length())
    }
    val addCol=udf(code)
    df.withColumn("other",addCol(df("name"))).show(false)*/
    //udf 第三种写法
    sparkSession.udf.register("strlen", (name: String, age: Long) => {
      getStringLength(name, age)
    })
    import sparkSession.implicits._
    df.withColumn("length", callUDF("strlen", $"name", $"age")).show()

    df.groupBy("name").agg(sum("age").as("age"))
      .select("name", "age").show()
  }

  //define the udf
  def getStringLength(name: String, age: Long) = {
    (name.length(), age + 1)
  }

  def udfTypeOperation(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/employees.json")
    //df.show()
    val averageSalary = UDFTypeAgg.toColumn.name("average_salary")
    val result = df.select(averageSalary)
    result.show()
  }

  def parquetFileOperation(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.json("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")
    df.write.parquet("people.parquet")
    val peopleDF = sparkSession.read.parquet("people.parquet")
    peopleDF.createOrReplaceTempView("parquetFile")
    val namesDF = sparkSession.sql("select name from parquetFile where age between 13 and 19")
    namesDF.show()
  }

  def hiveTableOperation(): Unit = {
    //指定warehouse的路径
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Hive Example")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport() //连接到Hive必须要有的配置
      .getOrCreate()
    import sparkSession.implicits._
    import sparkSession.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive") //创建数据表
    sql("LOAD DATA LOCAL INPATH '/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/kv1.txt' INTO TABLE src")
    //查询数据
    sql("SELECT * FROM src").show()
    //聚合操作
    sql("SELECT COUNT(*) FROM src").show()
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")
    //遍历每一行数据
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    //创建表并设置存储格式
    sql("CREATE TABLE hive_records(key int, value string) STORED AS PARQUET")
    val df = sparkSession.table("src")
    df.write.mode(SaveMode.Overwrite).saveAsTable("hive_records")
    //指定路径
    val dataDir = "/tmp/parquet_data"
    sparkSession.range(10).write.parquet(dataDir)
    //指定外部表
    sql(s"CREATE EXTERNAL TABLE hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    //开启动态分区
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    sparkSession.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    //创建分区表
    df.write.partitionBy("key").format("hive").saveAsTable("hive_part_tbl")
    sparkSession.sql("SELECT * FROM hive_part_tbl").show()
    sparkSession.stop()
  }

  def jdbcDataSource(): Unit = {

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("Spark Jdbc Example")
      .getOrCreate()
    //读取数据
    val jdbcDF = sparkSession.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/xiaoyao")
      .option("dbtable", "select * from xiaoyao.TBLS")
      .option("user", "root")
      .option("password", "123456")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    jdbcDF.show()
    val connectProperties = new Properties()
    connectProperties.put("user", "root")
    connectProperties.put("password", "123456")
    connectProperties.put("driver", "com.mysql.jdbc.Driver")
    val jdbcDF2 = sparkSession.read.jdbc("jdbc:mysql://localhost:3306/xiaoyao", "people", connectProperties)
    jdbcDF2.show()
    // 写入数据
    val schema = StructType(StructField("namge", StringType, false) :: StructField("age", LongType, false) :: Nil)
    val info = sparkSession.sparkContext.textFile("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    val rdd = info.map(_.split(",")).map(x => Row(x(0), x(1).toLong))
    val peopleDF = sparkSession.createDataFrame(rdd, schema)
    peopleDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/xiaoyao", "people", connectProperties)
    jdbcDF.show()
  }

  def otherTest(sparkSession: SparkSession): Unit = {
    val df = sparkSession.read.text("/Users/renren/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    df.show()
  }
}

case class Record(key: Int, value: String)