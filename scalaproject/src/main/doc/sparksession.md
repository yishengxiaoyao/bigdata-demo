#Spark Session
##介绍
Spark Session是编写DataSet和DataFrame的入口点。

###Spark Session作用 
* 可以创建DataSet和DataFrame，
* 读写外部数据源，
* 与metadata进行结合(catalog)
* 获取SparkConf
* 集群资源管理

catalog是一个接口，可以使用SparkSession.catalog来获取。
```
val sparkSession=SparkSession.builder()
  .appName("SparkSessionApp")
  .master("local[2]")
  .getOrCreate()
val catalog=sparkSession.catalog
catalog.listDatabases().show() //输出所有的数据库
catalog.listTables("default").show() //输出数据库的表
catalog.listColumns("default","emp").show() //输出表的列名
catalog.listFunctions().show() //输出所有的方法
sparkSession.stop()
```

Spark SQL从Spark1.0开始有的。
SchemaRDD是在Spark1.2之前,Spark1.3开始叫DataFrame，Spark1.6推出DataSet(编译时安全，将运行时异常编程编译时异常)。

| |SQL|DataFrame|DataSet|
|---|----|----|----|
|Syntax error|runtime|compile error|compile error|
|anlaysis error|runtime|runtime error|compile error|

spark.sql("seelect a from b") 只能在运行式才会去检查。

df.select(") 如果算子写错啦，就造成编译时错误，如果算子里面的值出现错误，就变成运行时错误。

ds.select(") 如果算子写错啦，就造成运行时错误，在进行ds编程时，尽量使用map的操作，可以将运行时错误编程编译时错误。
```
ds.map(_.itermId).show()
```

