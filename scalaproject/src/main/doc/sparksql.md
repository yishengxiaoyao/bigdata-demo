#Spark SQL Doc

##Spark SQL的简介

###1. 简介

Spark SQL是用于处理结构化数据的模块。与Spark RDD不同的是，Spark SQL提供数据的结构信息(源数据)和性能更好，可以通过SQL和DataSet API与Spark SQL进行交互。

###2. 特点

2.1 Spark程序和SQL可以无缝对接

2.2 统一数据访问：使用相同方式链接到数据

2.3 集成hive：在hive数据仓库中可以执行HQL，也可以执行SQL

2.4 标准连接：通过JDBC或者ODBC连接到数据

##Spark SQL应用场景

###1.执行SQL：从hive中读取数据

####Hive Table

>* Hive拥有很多依赖，但是不再Spark中，如果能从classpath中读取这些依赖，Spark会自动加载这些以来，如果是集群，在每个节点上都要有这些依赖，以便访问到Hive中的数据。
>* 需要将Hive的配置文件hive-site.xml(用来访问源数据)，拷贝到Spark的conf目录下。
>* 在读取Hive的数据时，要实例化一个SparkSession对象,用户不需要部署hive。如果没有配置hive-site.xml,Spark会自动在spark.sql.warehouse.dir(如果没有指定，在当前目录)指定的文件下创建metastore_db文件，在Spark2.0.0之后，spark.sql.warehouse.dir取代hive.metastore.warehouse.dir指定源数据的存放位置。
>* 如果使用Hive默认的数据库(derby),只能使用一个连接，因为derby是单session的。
>* 需要将相应的连接数据库的jar包，放到Hive和Spark的目录下，如果不这样做的话，可以在spark配置文件中指定的驱动程序的位置，在spark-defaults.conf文件，也可以使用--jars来引入驱动程序。
```
spark.driver.extraClassPath  ../jars/mysql-connector-java-bin-5.1.27.jar # jar包的位置
spark.executor.extraClassPath  ../jars/mysql-connector-java-bin-5.1.27.jar # jar包的位置
```
>* 配置驱动程序的方式：a)直接将驱动程序放入hive和spark的目录中；b)使用参数--jars;c)配置到spark-defaults.conf的文件中

####Spark SQL执行过程
>* 从Hive MetaStore中查找指定的数据库中是否有查找的数据表，如果有，执行下面的操作，如果没有返回错误。
>* Hive中的数据是存储在HDFS上，通过SparkContext来启动Job。
>* DAGScheduler 将DAG切分成为stage(多个task，taskSet)，并且提交stage 
>* TaskScheduler 在cluster manager启动task，如果task失败进行重试
>* Worker 运行task
>* WebUI 查看结果

###2.DataSet
>* DataSet是一个分布式数据集合，从Spark1.6开始添加的，可以从JVM或者通过transform操作获取DataSet。Python不支持DataSet API，Java和Scala支持。
>* DataFrame是对DataSet数据进行组织，类似一个二维表，具有字段名称和数据。可以通过结构化数据文件、Hive的数据表、外部数据源和已经存在的RDD中获取。Python、Java和Scala都支持DataFrame API，DataFrame借鉴自Python。
>* DataFrame=DataSet[Row],SchemaRDD(Spark1.2)==>DataFrame(Spark1.6)==>DataSet(Spark2.0.0)

##Spark SQL操作
###简介
1. 创建SparkSession对象:在创建的过程中会设置一些属性
```
var sparkSession=SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .getOrCreate()
```

2.获取DataFrame

2.1外部数据源
```
//Spark Sql支持的文件格式为：json, parquet, jdbc, orc, libsvm, csv, text
//如果是text的读取方式，会返回一个字符串列名字为value,其他的返回dataframe。
sparkSession.read.format("json").option("path","/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load().show()
sparkSession.read.json("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").show()
//如果读取的是csv文件，需要添加头信息
sparkSession.read.format("csv").option("sep", ";").option("header",true).option("inferSchema", "true").load("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.csv")
```
2.2从HiveTable中获取DataFrame
```
sparkSession.table("people").show()
```
需要将hive-site.xml文件拷贝到项目resources文件下，然后在pom文件中添加：
```
<dependency>
  <groupId>mysql</groupId>
  <artifactId>mysql-connector-java</artifactId>
  <version>5.1.27</version>
</dependency>
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-hive_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>
```
在创建SparkSession对象时，需要添加参数，用来支持Hive：
```
var sparkSession=SparkSession.builder()
      .appName("SparkSessionApp")
      .master("local[2]")
      .enableHiveSupport() //使用hive一定要开启这个
      .getOrCreate()
```
2.3从现有的RDD转换为DataFrame
```
import sparkSession.implicits._
sparkSession.sparkContext.parallelize(Array(("1", "xiaoyao"), ("2", "yisheng"),( "3", "bruce"))).toDF("id","name").show()
```
2.4spark读取的默认格式为parquet
```
println(sparkSession.conf.get("spark.sql.sources.default")) //输出为parquet,默认的数据源格式为parquet。
```
2.5输出schema信息
```
val df=sparkSession.read.format("json").option("path","/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
df.printSchema()
//printSchema()==>println(schema.treeString)
```

Spark1.5之后，Spark自己来管理内存而不是使用JVM，这样可以避免JVM GC带来的性能损失。内存中的Java对象被存储成Spark自己的二进制格式，计算直接繁盛在二进制格式上，省去了序列化和发序列化时间。
同时这种格式也更加紧凑，节省内存空间，而且能更好地估计数据量大小和内存使用情况。默认情况下为开启状态：spark.sql.tungsten.enabled=true。

3.获取指定的数据

3.1 获取一列数据
```
val df=sparkSession.read.format("json").option("path","/Users/Download/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
df.select("name").show()
//下面两种方式需要将sparkSession定义为val类型，需要导入隐士转换：import sparkSession.implicits._
df.select($"name").show() 
df.select('name).show() //需要进行隐士转换，在spark-shell中可以直接操作
df.select(col("domain")).show() //需要导入以来，import org.apache.spark.sql.functions.col
```
3.2对某些列进行操作
```
df.select('name,'age+1).show() //需要进行隐士转换，在spark-shell中可以直接操作
```
3.3对数据进行过滤
```
df.filter("age>21").show() //age>20
df.filter('age>21).show()  //age>20
df.filter($"age">21).show() //age>20
df.filter('age===19).show() //age==19
df.filter($"age"===19).show() //age==19
df.filter("age=19").show() //age==19
df.filter("age==19").show() //age==19
```
在上面的参数中show(),默认展示20条数据，可以指定设置展示的个，第二个参数为是否对字符串进行裁剪(如果字符串的长度超过20个，就需要这个参数truncate)。
3.4 聚合函数
```
df.groupBy("domain").agg(sum("responseSize").as("rs"))
      .select("domain","rs").show() //后面的agg表示执行聚合操作
```

4.创建数据的视图
由于数据的视图是与SparkSession的同生命周期(SparkSession结束，视图失效)，可以创建一个全局的视图，可以在全局使用，然后就可以使用sql来操作数据
```
df.createOrReplaceGlobalTempView("people")
    sparkSession.sql("SELECT * FROM global_temp.people").show() #不要同时在Spark-shell与idea同时运行
```
5.与RDD进行交互
有两种方式将RDD转换为DataSet：

a)使用反射来推断出来对象的类型，这种方式可以减少代码量，如果知道数据类型之后，可以提高性能；
```
def inferReflection(sparkSession: SparkSession): Unit ={
    //get the origin data
    val info=sparkSession.sparkContext.textFile("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    //split the data and convert to an object
    import sparkSession.implicits._
    //简单写法
    //val df=info.map(_.split(",")).map(x=>People(x(0),x(1).toLong)).toDF()
    //复杂写法,在使用这种方式时，需要表明每一列的名称
    val df=info.map(x=>{
      val temp=x.split(",")
      (temp(0),temp(1).toLong)
    }).toDF("name","age")

    //Before Spark2.0,we can operate directly.Since Spark2.0,we need to convert the dataframe to rdd.
    //df.map(x=>x(0)).collect().foreach(println) //before spark2.0
    //df.rdd.map(x=>x(0)).collect().foreach(println) //since spark2.0
    //df.rdd.map(x=>x.get(0)).collect().foreach(println)
    //df.rdd.map(x=>x.getAs[String](0)).collect().foreach(println)
    //df.rdd.map(x=>x.getString(0)).collect().foreach(println)
    //df.rdd.map(x=>x.getAs[String]("name")).collect().foreach(println)
    //df.select(col("name")).show()
    /**
      * Compute the average for all numeric columns grouped by department.
      * ds.groupBy("department").avg()
      * // Compute the max age and average salary, grouped by department and gender.
      * ds.groupBy($"department", $"gender").agg(Map(
      * "salary" -> "avg",
      * "age" -> "max"
      * ))
      */
    df.groupBy("name").agg(sum("age").as("age"))
     .select("name","age").show()
  }
  //scala2.10之前，字段个数有限制：最大值为22个,在scala2.10之后，就没有这个限制，如果超过这个限制，修改设计或者使用nested case classes。
case class People(name:String,age:Long) 
```
b)通过编程方式，构建schema结构，然后作用与现有的RDD，这种方式在运行时才会知道字段的类型。
```
def programmatically(sparkSession: SparkSession): Unit ={
    /**
      * 1.Create an RDD of Rows from the original RDD;
      * 2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
      * 3.Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
      */
    //1.Create an RDD of Rows from the original RDD;
    val info=sparkSession.sparkContext.textFile("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
    val rdd=info.map(_.split(",")).map(x=>Row(x (0),x(1).toLong))
    //2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
    val struct= StructType(
      StructField("name", StringType, false) ::
        StructField("age", LongType, false) :: Nil)
    //第二中写法
    /*val struct= StructType(
                Array(StructField("name", StringType, false),
                  StructField("age", LongType, false)))*/
    //Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
    val df=sparkSession.createDataFrame(rdd,struct)
    //df.show()
    df.groupBy("name").agg(sum("age").as("age"))
      .select("name","age").show()
  }
```
c)聚合函数
```
df.groupBy("name").agg(Map("age"->"max")).show()
```
d)自定义函数
```
自定义函数的步骤：1.defined the udf；2.register the udf；3.use the udf
1.defined the udf
def getStringLength(name: String)={
    name.length()
}
2.register the udf
sparkSession.udf.register("strlen",(name:String)=>{
      getStringLength(name)
})
3.use the udf
//Registers this Dataset as a temporary table using the given name,
// registerTempTable is deprecated since 2.0.0,we can use createOrReplaceTempView instead.
df.registerTempTable("people")
df.createOrReplaceTempView("people")
sparkSession.sql("select name,strlen(name) from people").show()
```
e)增加字段
```
//使用sql的方式
sparkSession.udf.register("strlen",(name:String,age:Long)=>{
  getStringLength(name,age)
})
df.registerTempTable("people")
df.createOrReplaceTempView("people")
sparkSession.sql("select name,strlen(name) as length from people").show()
//udf第二种方式
val code=(args0:String)=>{
  (args0.length())
}
val addCol=udf(code)
df.withColumn("other",addCol(df("name"))).show(false)
//udf 第三种写法
sparkSession.udf.register("strlen",(name:String,age:Long)=>{
  getStringLength(name,age)
})
import sparkSession.implicits._
df.withColumn("length",callUDF("strlen",$"name",$"age")).show()
```
f)创建DataSet
```
DataSet使用具体的编码器而不是用java和kryo的序列化方式。
编码器和序列化都是将对象变成字节，编码器是动态产生并且可以执行过滤、排序和hash等操作而不用反序列化为对象。
import sparkSession.implicits._
//读取数据，直接对应到类
//val peolpleDs = sparkSession.read.json("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").as[People]
//peolpleDs.show()
//将数据直接序列化
val ds = Seq(People("Andy", 32)).toDS()
ds.show()
```


6.将数据写入到文件中
```
//在写文件时，只能指定文件的路径，不能指定文件的名称
val df=sparkSession.read.format("json").option("path","/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
df.select("name").write.format("json").mode(SaveMode.Overwrite).save("/Users/Downloads/test/")
//也可以指定不同的格式    
df.select("name").write.format("text").mode(SaveMode.Overwrite).save("/Users/Downloads/test/")
//写入orc文件：
df.write.format("orc").option("orc.bloom.filter.columns", "favorite_color").option("orc.dictionary.key.threshold", "1.0").save("users_with_options.orc")
//保存为HiveTable,也可以指定路径
val df=sparkSession.read.format("json").option("path","/Users//Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json").load()
df.select("name").write.saveAsTable("people")
//sparkSession.sql("select * from people").show()
sparkSession.table("people").show()
```
6.1 SaveMode

|Scala/Java|Any Language|Meaning|
|---|---|---|
|SaveMode.ErrorIfExists(默认)|"error" or "errorifexists"|如果文件存在，就报错|
|SaveMode.Append|"append"|如果文件存在，直接追加，如果不存在，创建|
|SaveMode.Overwrite|"overwrite"|如果存在直接重写，如果不存在，创建|
|SaveMode.Ignore|"ignore"|如果存在，不写|

6.2 Saving to Persistent Tables

DatFrame使用saveAsTable将数据存储到HiveMetaStore中(不需要安装Hive)。如果Spark应用程序重启之后可以连接到之前HiveMetaStore，数据依然存在。

使用df.write.option("path", "/some/path").saveAsTable("t")，如果自定义路径之后，删除表之后，路径和数据依然存在。如果没有自定路径，删除表之后，表和数据都没有拉。

从Spark2.1之后，可以存储分区表，有以下好处：
>* 只返回查询的分区，不用扫描所有的分区
>* 在DataSource 中，可以直接使用Hive DDLs
在默认情况，创建外部表的时候，不会存储分区信息，需要使用命令MSCK REPAIR TABLE来将这些信息加上。

6.3 Bucketing, Sorting and Partitioning

Bucketing和分区只能在持久化表的时候，才能使用
```
peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
```
在使用DataSet API时，可以使用save和saveAsTable。
```
usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
```
在操作单表的时候，可以使用partitioning和bucketing
```
usersDF
  .write
  .partitionBy("favorite_color")
  .bucketBy(42, "name")
  .saveAsTable("users_partitioned_bucketed")
```

7.直接运行SQL
```
sparkSession.sql("select * from parquet.`/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/users.parquet`").show()
```

###数据源操作
1.parquet
1.1读取数据保存为parquet格式
```
val df=sparkSession.read.json("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.json")
    df.write.parquet("people.parquet")
    val peopleDF=sparkSession.read.parquet("people.parquet")
    peopleDF.createOrReplaceTempView("parquetFile")
    val namesDF=sparkSession.sql("select name from parquetFile where age between 13 and 19")
    namesDF.show()
```
1.2发现分区

内置文件数据源Text/CSV/JSON/ORC/Parquet可以自动发现分区和获取分区信息。
SparkSql可以从路径中自动发现分区信息和分区字段的类型，目前支持类型为：numeric data types、date、timestamp、string。
如果不需要字段的类型，设置参数spark.sql.sources.partitionColumnTypeInference.enabled=false。
Spark1.6开始支持发现分区，只查找指定路径下的分区。如果需要更深层次的分区，需要指定basePath。

1.3Schema Merging

从SparkSQL1.5开始该功能默认不开启，非常耗内存。如果需要的话，可以开启，设置spark.sql.parquet.mergeSchema=true。
Parquet数据可以自动对数据的schema信息进行合并。

1.4Hive metastore Parquet table conversion

Hive与Parquet在处理表schema信息的区别：
a)Hive不区分大小写，Parquet区分大小写；
b)Hive需要考虑列是否为空，Parquet不需要考虑；
在将Hive的Parquet数据表转换为SparkSQL的Parquet表时，需要将Hive中的源数据与Parquet的源数据进行转换。
转换规则为：仅出现在Parquet中出现的源数据需要考虑，仅出现在Hive源数据中，可以添加为null值列。

1.5刷新MetaData
为了提高性能，需要对Parquet数据进行缓存，Hive metastore Parquet table conversion时，会造成一些源数据发生变化，需要刷新代码中缓存的源数据，使用
```
sparkSession.catalog.refreshTable("my_table")
```

1.6配置

常用的配置有：

|参数|解释|
|----|----|
|spark.sql.parquet.binaryAsString| 为了兼容之前的版本和其他的源数据，将二进制变成字符串，默认为不开启|
|spark.sql.parquet.int96AsTimestamp| 将其他系统中的INT96转换为SparkSQL的timestamp类型，默认为转换|
|spark.sql.parquet.compression.codec| 对Parquet数据进行压缩|
|spark.sql.parquet.filterPushdown|用于过滤优化|

1.7 Parquet文件的优点
* 高效，Parquet采取列示存储避免读入不需要的数据，具有极好的性能和GC。
* 方便的压缩和解压缩，并具有极好的压缩比例。
* 可以直接固化为parquet文件，可以直接读取parquet文件，具有比磁盘更好的缓存效果。

2.ORC文件
从Spark2.3支持矢量化读取ORC文件，需要开启一下配置：

|参数|解释|
|----|----|
|spark.sql.orc.impl|native和Hive二选一，native是基于ORC1.4,Hive是基于Hive的ORC1.2.1|
|spark.sql.orc.enableVectorizedReader|默认为true，在本地可以矢量化读取，否则，不可以，如果读取hive数据，忽略这个配置|

3.JSON

在读取JSON文件时，分单行和多行读取，如果使用多行读取，需要将multiline配置设置为true。

4.HiveTable
在从Hive总读取数据时，需要指定spark.sql.warehouse.dir的地址。
可以创建Hive表、加载数据、分区
```
//指定warehouse的路径，需要导入import java.io.File
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
case class Record(key: Int, value: String)
```
4.1设置文件的存储格式
默认情况下，读取Hive Table都是以文本文件读取。Hive storage handler不支持创建表(spark sql中)，在hive中创建表，使用spark sql读取。

|参数|解释|
|----|---|
|fileFormat|存储文件格式：sequencefile、rcfile、orc、parquet、textfile、avro。|
|inputFormat, outputFormat|设置输入格式和输出格式|
|serde|进行序列化的方式|
|fieldDelim, escapeDelim, collectionDelim, mapkeyDelim, lineDelim|指定分隔符|

4.2与Hive MetaStore进行交互
从SparkSQL1.4可以访问各种Hive MetaStore。

|参数|解释|
|----|---|
|spark.sql.hive.metastore.version|默认Hive版本为1.2.1,支持0.12.0到2.3.3|
|spark.sql.hive.metastore.jars|实例化HiveMetastoreClient的三种方式：内置(必须要定义spark.sql.hive.metastore.version)、maven(不推荐使用)、classpath(必须要包含所有以来)|
|spark.sql.hive.metastore.sharedPrefixes|连接数据库驱动程序|
|spark.sql.hive.metastore.barrierPrefixes|连接不同版本的Hive|


5.JDBC

|参数|解释|
|----|---|
|url|连接字符串|
|dbtable|连接的表|
|query|查询语句，dbtable不能与query同时使用，query不能与partitionColumn同时使用|
|driver|连接驱动类|
|numPartitions|同时读取分区的数量，同时最大连接数，如果数量超过限制，会对数据进行重新分配|
|partitionColumn, lowerBound, upperBound|查询条件限制，按照那个字段进行分区，扫描数据的范围|
|fetchsize|返回数据的大小|
|batchsize|一次获取数据的大小|
|createTableOptions|创建表时指定分区等一系列参数|
|pushDownPredicate|是否使用谓词下压，默认为true|
设置连接相关参数的方式：1.使用option来指定；2.通过Properties来指定。

```
val sparkSession=SparkSession
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
  .option("driver","com.mysql.jdbc.Driver")
  .load()
jdbcDF.show()
val connectProperties=new Properties()
connectProperties.put("user","root")
connectProperties.put("password","123456")
connectProperties.put("driver","com.mysql.jdbc.Driver")
val jdbcDF2=sparkSession.read.jdbc("jdbc:mysql://localhost:3306/xiaoyao","people",connectProperties)
jdbcDF2.show()
// 写入数据
val schema= StructType(StructField("namge", StringType, false) :: StructField("age", LongType, false) :: Nil)
val info=sparkSession.sparkContext.textFile("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
val rdd=info.map(_.split(",")).map(x=>Row(x (0),x(1).toLong))
val peopleDF=sparkSession.createDataFrame(rdd,schema)
peopleDF.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/xiaoyao","people",connectProperties)
jdbcDF.show()
```

6.Troubleshooting

>* 原始class loader必须要对JDBC Driver可见，因为Java有安全检查；修改classpath.sh，使得classpath.sh包含driver的jar。

>* 一些数据库会将名字变成大写。在Spark SQL需要将名字变成大写。

>* 用户可以通过指定具体的JDBC来连接到相应的数据库。


7.分布式的SQL Engine

7.1运行Thrift JDBC/ODBC服务

Spark SQL的Thrift JDBC服务，兼容现有Hive，不需要修改Hive元数据，或者改变表中数据位置或者表的分区。
>* 通过环境修改HiveServer2端口(默认为10000): export HIVE_SERVER2_THRIFT_PORT=10000,export HIVE_SERVER2_THRIFT_BIND_HOST=localhost，启动./start-thriftserver.sh --master local[2]。
>* 通过--hiveconf指定Hive属性:./start-thriftserver.sh --hiveconf hive.server2.thrift.port=10000 --hiveconf hive.server2.thrift.bind.host=localhost --master local[2]

8.性能调优

8.1 缓存数据

缓存数据:sparksql调用sqlContext.cacheTable("tableName")或者使用dataFrame.cache()构建一个内存中的列式缓存表，Spark SQL仅扫描需要的列，并自动调整压缩比，使内存使用率和GC压力最小化。使用uncacheTable("tableName")来一处缓存。

|属性|默认值|含义|
|----|----|----|
|spark.sql.inMemoryColumnarStorage.compressed|true|设置为true，SparkSQL将基于数据统计为每列自动选择压缩编码|
|spark.sql.inMemoryColumnarStorage.batchSize|10000|控制列式缓存的批处理尺寸，大批量可以提高内存的使用率和压缩率，但是缓存数据时会有内存溢出的风险|

8.2
##自定义外部数据源
###名词解释
1.BaseRelation

Schema是一个KV的数据组合，继承BaseRelation的类，必须要设置自己的Schema(StructType形式)，实现Scan类的相关方法。主要功能是定义Schema信息。

2.TableScan

将元祖数据变成Rows对象中的数据。

3.PrunedScan

将不需要的列过滤掉。

4.PrunedFilteredScan

先进行数据过滤然后将不需要的列过滤掉。

5.InsertableRelation

将数据写回去。

有三种假设：a)插入字段与原先的字段一致；b)Schema细腻不能变化；c)插入的数据可以为空。

6.RelationProvider

指定数据源，如果没有找到指定的数据源，就找DefaultSource附加到路径中，每次调用DDL都会创建实例对象。

7.DataSourceRegister

注册自定义的外部数据源

关于自定义数据源请参考后续文章。

##问题总结
###DataFrame vs RDD
1.RDD

存储：只知道存储数据的类型

运行：运行在JVM上

2.DataFrame

存储：相当于一个二维表，含有列的信息(列的信息，类型)，相当于Schema信息，根据这些信息进行优化，例如：列裁剪、行过滤等，与外部数据源可以方便结合。

运行：运行时会转换成为Logic Plan=>Physical Plan =>execut plan，在这中间进行了优化

### Spark on Hive
这种说法是错误的。

> * Hive的执行引擎有多个：MapReduce、Tez、Spark。在Hive中编写SQL之后，可以在不同的执行引擎中执行，就是转换为相应的运行任务，用户是无感知的。
从Hive的角度出发，Hive出来的时间比较长，功能比较完善，对SQL(HQL)的支持粒度更细。

> * 在Spark中，有一个专门的模块SparkSQL可以使用SQL查询Hive中的数据，SparkSQL出来的时间比较短，对SQL支持粒度比较粗。


### Spark一定需要Hadoop
Spark可以不需要Hadoop，Spark可以在其他的地方执行。

### Spark能替代Hadoop
不能，Spark是一个分布式计算引擎，没有地方存储数据。Spark可以访问Hadoop中的数据，Spark任务在YARN中调度。

### Spark 需要Hive吗
不需要，只需要有MetaSore服务就行。

### thriftserver vs spark Application
>* spark application：一个sparksubmit包含一个sparkcontext，多个客户端不能共享一个应用程序(sparkcontext.close)，每次都要申请资源
>* thriftserver：可以共享一个server，是一个长服务，只要server存在，client都可以链接，只需要申请一次资源，后面可以直接使用。

### Spark操作Hive数据：connection refused
Hive中的数据存放HDFS中，如果可以连接到数据库中，出现这种情况就是Hadoop没有启动。


### RDD vs DataFrame and DataSet
https://data-flair.training/blogs/apache-spark-rdd-vs-dataframe-vs-dataset/
https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html

###Spark Fault Tolerance
https://data-flair.training/blogs/fault-tolerance-in-apache-spark/

###需求
需求：
1.
读取json数据，放在HDFS上  注册成一张表
读取MySQL中数据 sqoop读取到Hive中，放到HDFS上
将上面的数据进行join操作
2.json==>parquet/orc 读取json数据插入到parquet表中，进行gzip/bzip2压缩
3.oracle有一个大表，