# Spark Core
## Spark 概述
1. Spark 是用于处理大数据的统一分析引擎。
2. Spark 特性
> * Speed:Spark在批处理和流式数据处理方面性能优势比较大，使用DAG调度、查询优化、物理执行引擎。
> * 使用简单：可以和多种语言进行交互。
> * Generality:可以与SQL、streaming(流)、或者复杂分析进行结合。
> * Runs Everywhere：Spark可以执行在hadoop、mesos、kubernetes、standalone或者云，以及可以访问其他的多样数据源。

3.RDD

RDD(Resilient Distributed Dataset，离散分布式数据集)是Spark的基石。展现形式为不可变的(A=>操作=>B，A与B不同)、对分区的数据进行并行操作。

RDD的特性：
> * 多个分区 (protected def getPartitions: Array[Partition],这个方法只会被调用一次，是安全的,必须要在子类实现)
> * 计算的操作作用于每一个分区(def compute(split: Partition, context: TaskContext): Iterator[T]，必须要在子类实现)
> * RDD依赖与其他的RDDs (protected def getDependencies: Seq[Dependency[_]] = deps)
> * KV RDD的分区器(默认为hash分区) (@transient val partitioner: Option[Partitioner] = None)
> * 在那个最好的分区上执行计算操作 (protected def getPreferredLocations(split: Partition): Seq[String] = Nil,必须要在子类实现)

4.  Spark的两个抽象

4.1 RDD:可以在集群上进行并行操作已经分区的元素集合。

以HadoopRDD为例，介绍RDD的5大特性：
> * getPartitions: a.获取配置；b.获取输入格式。使用反射方式(ReflectionUtils),然后使用模式匹配获取输入格式;c.获取输入的分片数量；
> * compute:使用迭代器获取数据，读取HDFS的数据返回是KV的值，K表示offset，V表示是内容。
> * getDependencies: HadoopRDD中没有实现这个方法。HDFS数据是源头，没有其他RDD的依赖。

4.2 共享变量：每个任务共享的参数或者值。

## 初始化Spark

1. 添加依赖
```
<dependency>
  <groupId>org.apache.hadoop</groupId>
  <artifactId>hadoop-client</artifactId>
  <version>2.6.0-cdh5.7.0</version>
</dependency>
```
2. 创建对象

创建一个SparkContext对象，设置怎么访问集群。在当前JVM中只能存在一个active的SparkContext实例。创建SparkConf对象。

2.1 SparkConf 

Spark Conf是用来获取Spark应用程序的配置信息，配置信息都是键值对信息。

在创建SparkConf实例时，读取以spark开头的配置(spark.*，如果设置自定义参数，必须以spark开头)，如果不是，就会忽略这些参数。可以对参数直接设置。然后加载到sparkconf中。

在进行单元测试时，需要使用new SparkConf(false)跳过外部配置，获取系统配置。

创建SparkConf对象
```
/**
  * 在启动的spark应用程序时，程序会对appName和master检查是否设置，如果没有设置报错
  * if (!_conf.contains("spark.master")) {
  * throw new SparkException("A master URL must be set in your configuration")
  * }
  * if (!_conf.contains("spark.app.name")) {
  * throw new SparkException("An application name must be set in your configuration")
  * }
  * 在idea中必须要设置appName，在shell中不用设置，必须要设置master
  * def setMaster(master: String): SparkConf = {
  * set("spark.master", master)
  * }
  * def setAppName(name: String): SparkConf = {
  * set("spark.app.name", name)
  * }
  * 在编写程序时，不要对appName和master进行硬编码。
  */
val sparkConf=new SparkConf().setAppName("SparkContextApp").setMaster("local[2]")
```
spark-submit相关的参数都在SparkSubmitArguments.scala类里面中。也可以使用export方式来设置配置，例如export MASTER="local[2]"，在环境变量中设置。

在配置文件中获取SPARK_CONF_DIR参数。

Spark硬编码的参数优先级要高于其他配置的参数。

2.2 SparkContext

SparkContext是Spark的一个主要入口点。创建完SparkContext实例之后，用来创建RDD和广播变量。在当前JVM中只能存在一个active的SparkContext实例。

如果想要创建新的SparkContext实例，需要先将之前的SparkContext实例关掉。

SparkContext对象代表者连接到Spark集群中。

SparkContext:
> * 创建SparkConf 获取配置
> * 创建SparkEnv 获取环境变量  创建DAG
> * DAGScheduler 将DAG切分成为stage(多个task，taskSet)，并且提交stage 
> * TaskScheduler 在cluster manager启动task，如果task失败进行重试
> * Worker 运行task
> * WebUI 查看结果

创建对象
```
val sc=new SparkContext(sparkConf)
```

2.3 DAGScheduler

DAGScheduler是面向stage调度的高阶调度层。计算每个作业的每个阶段的DAG,跟踪哪些RDD和stage输出可以物化(写到磁盘或者其他地方)，找到最少的调度。

Stage都是以TaskSets的形式来提交，并且运行在集群上。

在shuffle操作边缘打断RDD图来产生stage。map、filter都是窄依赖，多个任务串联起来形成一个stage，shuffle操作依赖多个stage。每个stage只有一个shuffle依赖于其他stage。

DAGScheduler决定任务在那个节点上运行，另外，重试作业在作业失败的时候。

|concepts|meaning|
|----|----|
|Jobs|jobs是顶级调度的基本单位|
|Stages|多个task的集合，stage通过shuffle操作来进行分割，stage类型：ResultStage(用于执行动作的最后阶段)和ShuffleMapStage(将数据写出用于shuffle)|
|Task|work的独立单元|
|Cache tracking|缓存数据防止再次重新计算|
|Preferred locations|在比较好的地方执行任务，如数据在当前节点上|
|Cleanup|在job执行完自后，将剩余的内容清除|

2.4 TaskScheduler

TaskScheduler是水平的任务调度接口。

通过使用SchedulerBackend调度多种cluster manager的任务。

支持的调度方式：FIFO(默认的)，FAIR。

Spark的推测执行默认为关闭的。设置任务的重试次数。使用spark.speculation.interval设置推测间隔。推测式运行式从后往前开始推算。
## RDD

Spark紧紧围绕RDD来编程，RDD是一个容错的数据集并且可以并行操作。

创建RDD的两种方式：通过并行集合方式；通过外部的存储系统；RDD通过map操作形成新的RDD。

1. 并行化集合

parallelize：将本地的scala分布式数据集变成一个RDD。这个方法的第二参数表示是并行度，默认值为2。

```
val rdd=sc.parallelize(Array(1,2,3,4,5))
//由于collect操作返回是一个Array，如果需要获取结果，需要对其进行遍历，foreach也可以认为是一个action
rdd.collect().foreach(println)
```

2. 外部数据集

Spark支持文本文件、SequenceFile、和Hadoop支持输入格式。

读取文本文件，需要设置uri(s3a://,hdfs://,file:///)。

读取文件重点：
> * 读取本地文件系统，这个文件路径必须在所有work node上都存在。 
> * 基于文件的input method，支持路径、压缩文件、使用通配符。
> * textFiles的第二个参数用于控制文件的分区数。默认情况下，Spark可以创建文件的分区(默认HDFS文件大小为128M)。但是分区数不能小于块的数量。分区的数量就是task的数量。
> * SparkContext.wholeTextFiles可以读取多个小文本文件，返回值为(文件名，内容)。textFile是返回文件的每一行数据。
> * Hadoop的其他输入格式，都可以使用HadoopRDD来处理。
> * RDD.saveAsObjectFile和SparkContext.objectFile都可以将RDD保存为序列化数据。

分区数会影响最后生成文件的数量：分区数大，速度快，生成的小文件多；分区数小，速度慢，生成的小文件少。

```
//textFile需要所有的节点上在相同的路径拥有相同的文件
val textRdd=sc.textFile("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
//由于collect操作返回是一个Array，如果需要获取结果，需要对其进行遍历，foreach也可以认为是一个action
textRdd.collect().foreach(println)
/**
  * 从HDFS或者本地文件系统中读取文本文件，返回值为KV形式的(文件名称，内容)
  */
val otherRdd=sc.wholeTextFiles("/Users/Documents/test/spark-2.2.2-bin-2.6.0-cdh5.7.0/examples/src/main/resources/people.txt")
otherRdd.collect().foreach(println)
```
##Spark Shell

1.参数介绍
> * master 运行在那儿，在代码中不要进行硬编码
> * deploy-mode 部署的模式，是否在本地启动驱动程序
> * class 应用程序的主类
> * name 应用程序的名称,默认为spark-shell
> * jars 添加一下需要的jars

在Spark-Shell启动之后，会自动创建一个SparkContext实例，可以在页面(ip:4040)上看到,如果开启多个spark-shell，查看相应的页面，需要在端口后面进行累加。

2.Spark-Shell 运行过程

指定SPARK_HOME
指定SPARK_SUBMIT_OPTS，启动spark-submit,指定class和name，以及其他参数
```bash
export SPARK_SUBMIT_OPTS
"${SPARK_HOME}"/bin/spark-submit --class org.apache.spark.repl.Main --name "Spark shell" "$@"
```
执行spark-class
```bash
exec "${SPARK_HOME}"/bin/spark-class org.apache.spark.deploy.SparkSubmit "$@"
```
加载环境变量:在conf文件下查找spark-env.sh文件和查找相关的scala文件。
```bash
. "${SPARK_HOME}"/bin/load-spark-env.sh
```
查找JAVA_HOME、Spark Jars
```bash
if [ -n "${JAVA_HOME}" ]; then
  RUNNER="${JAVA_HOME}/bin/java"
else
  if [ "$(command -v java)" ]; then
    RUNNER="java"
  else
    echo "JAVA_HOME is not set" >&2
    exit 1
  fi
fi

# Find Spark jars.
if [ -d "${SPARK_HOME}/jars" ]; then
  SPARK_JARS_DIR="${SPARK_HOME}/jars"
else
  SPARK_JARS_DIR="${SPARK_HOME}/assembly/target/scala-$SPARK_SCALA_VERSION/jars"
fi
```

3.说明

Spark-Shell会自动创建一个SparkContext实例，简称为sc。

只要遇到action操作就会产生一个新的job。

spark提交程序的流程：spark-shell->spark-submit->spark-class。

## RDD 操作

RDD支持的两个操作：transformations:产生一个新的数据集；actions:将结果返回到程序中。

所有的transformations操作都是lazy的。transformations只有在遇到actions操作时才会执行。这样提高性能。

可以将数据缓存(persist/cache)到内存中。

本地模式与cluster模式不同：
> * 在计数的时候，本地模式直接使用变量统计，cluster模式需要使用accumulator，
> * 输出数据：本地模式可以直接输出，cluster模式需要将数据收集到一个节点上，然后输出

## Transformations

|Transformation|Mean|
|---|----|
|map|通过一些操作生成新的RDD，对于每一个元素都进行操作|
|filter|将选择的数据进行过滤之后，生成新的RDD|
|flatmap|将数据扁平化，然后在处理，生成一个序列|
|mapPartitions|在每个partitions上都要执行的操作，每个partition执行一次操作，然后返回一个新的RDD，适用于多分区|
|mapPartitionsWithIndex|操作函数会返回一个分区的索引，作用于每一个分区|
|sample|返回RDD的子集|
|union|两个数据集进行union操作|
|intersection|两个数据集进行交操作|
|distinct|返回数据集中只有一个的数据|
|groupByKey|如果按照某个key进行聚合操作，可以使用reduceByKey或者aggregateByKey提高性能。默认情况下，并行度是与分区一致。尽量不要使用这个算子，尽量使用reducebykey来代替|
|reduceByKey|按照key进行聚合操作|
|aggregateByKey|使用特定的方法对key进行聚合操作|
|sortByKey|按照key进行排序|
|join|两个数据集执行join操作|
|coalesce|减少分区数，将大数据集的数据量减少|
|repartition|随机调整数据集的数据，平分数据|
|repartitionAndSortWithinPartitions|调整数据分布，在分区中进行排序|
map是对数据集中的每一个元素进行操作，mapPartitions是对partition进行操作，
如果是需要获取数据库连接，使用mapPartitions算子，但是需要内存的分配，否则会出现oom。
```
val sparkConf = new SparkConf()
  .setMaster("local[2]").setAppName("MapPartitionApp")
val sc = new SparkContext(sparkConf)
val students = new ListBuffer[String]()
for(i <- 1 to 100) {
  students += "student：" + i
}
/**
  * map操作
  */
/*val stus = sc.parallelize(students, 4)
stus.map(x => {
  val connection =  DBUtils.getConnection()
  println(connection + "~~~~~~~~~~~~")
  DBUtils.returnConnection(connection)
}).foreach(println)*/
/**
  * mapPartitions操作，需要定义并行度
  */
val stus = sc.parallelize(students, 4)
stus.mapPartitions(partition => {
 val connection =  DBUtils.getConnection()
 println(connection + "~~~~~~~~~~~~")
 DBUtils.returnConnection(connection)
 partition
}).foreach(println)
sc.stop()
```
coalesce可以减少分区数量，要变成的分区数量必须要原本的分区数要小；
repartition可以变成指定的分区数，数据平均分配。repartition底层调用coalesce，并且有shuffle操作。
```
val sparkConf = new SparkConf()
  .setMaster("local[2]").setAppName("CoalesceRepartitionApp")
val sc = new SparkContext(sparkConf)

val students = new ListBuffer[String]()
for(i <- 1 to 100) {
  students += "student：" + i
}

val stus = sc.parallelize(students, 3)
//获取partition的数量
println(stus.partitions.size)
stus.mapPartitionsWithIndex((index, partition) => {
  val emps = new ListBuffer[String]
  while(partition.hasNext) {
    emps += ("~~~" + partition.next() + " ,old:[" + (index+1) + "]")
  }
  emps.iterator
}).foreach(println)

println("============coalesce================")
stus.coalesce(2).mapPartitionsWithIndex((index, partition) => {
  val emps = new ListBuffer[String]
  while(partition.hasNext) {
    emps += ("~~~" + partition.next() + " , new:[" + (index+1) + "]")
  }
  emps.iterator
}).foreach(println)
println("============repartition================")

stus.repartition(5).mapPartitionsWithIndex((index, partition) => {
  val emps = new ListBuffer[String]
  while(partition.hasNext) {
    emps += ("~~~" + partition.next() + " , new:[" + (index+1) + "]")
  }
  emps.iterator
}).foreach(println)

sc.stop()
```
在进行对key进行求和时，需要使用reduceByKey，如果需要使用groupByKey，需要先做一步处理，使用combineByKey。

groupbyKey不进行本地合并，而进行聚合操作的reduceByKey会在本地对每个分区的数据进行合并在进行shuffle，效率比groupbykey高的多。

## Actions

|Actions|Mean|
|-----|-----|
|reduce|对数据集中的元素进行聚合操作(两个参数一个返回值)，处理的方法可以交互和关联的|
|collect|将数据集的元素以数组的形式返回到driver中|
|count|统计数据集中的元素|
|first|获取数据集中第一个元素，等同于take(1)|
|take(n)|获取前N个元素|
|takeSample|获取数据集中有指定元素的子集|
|takeOrdered|使用哪种排序方式，获取前N个元素|
|saveAsTextFile|将数据存储到文本文件中|
|saveAsSequenceFile|将数据写入到SequenceFile|
|saveAsObjectFile|使用java序列化的简化格式来编写数据集的元素，可以使用SparkContext.objectFile()来加载|
|countByKey|仅适用于(K,V)形式，返回(K,int)|
|foreach|对每个元素进行操作|
```
val sparkConf = new SparkConf()
  .setMaster("local[2]").setAppName("ForeachPartitionApp")
val sc = new SparkContext(sparkConf)
val students = new ListBuffer[String]()
for(i <- 1 to 100) {
  students += "student：" + i
}
val stus = sc.parallelize(students, 4)
/**
  * foreach是对每一个数据进行操作
  */
stus.foreach(x => {
  val connection =  DBUtils.getConnection()
  println(connection + "~~~~~~~~~~~~")
  DBUtils.returnConnection(connection)
})
/**
  * foreachPartition 对每个partition进行操作
  */
stus.foreachPartition(partition => {
 val connection =  DBUtils.getConnection()
 println(connection + "~~~~~~~~~~~~")
 DBUtils.returnConnection(connection)
})
sc.stop()
```

## Shuffle Operation

Shuffle操作就是重新分配数据或者说是将分区中的数据进行分组。这个操作会牵扯到机器之间的数据传输，这是成本很高的操作。

从分区中读取所有的key和value，然后将相同的key放到同一个地方，然后计算，获取最终的结果，这个过程就是shuffle。

shuffle操作会提高磁盘I/O、数据序列化、提高网络I/O。

一些shuffle操作会消耗大量的内存，就是在传输之前，将数据存储到内存中。如果数据量大，将数据写入到磁盘上，造成回收变的频繁。

Shuffle操作也会产生大量的中间文件。


## 窄依赖

一个父RDD的partition至少会被子RDD的某个partition使用一次。map、filter、union操作是窄依赖

## 宽依赖

一个父RDD的partition会被子RDD的partition使用多次。groupBykey、reduceBykey操作是宽依赖。


join操作：join with inputs co-partitioned(窄依赖，两个数据集使用相同的分区器)、join  with inputs not co-partitioned(宽依赖，使用不同的分区器)

遇到宽依赖就会产生一个新的stage。没有宽依赖就没有shuffle。


## RDD Persist

缓存数据会提高运行速度。缓存数据(是一个lazy操作，遇到action才会执行)可以使用persist()或者cache(),遇到action才会执行，缓存数据是一个容错操作，如果缓存丢掉，会立即重新计算的。

cache()=>persist()=>persiste(StorageLevel.MEMORY_ONLY), cache在sparksql中是积极(eager)的，在sparkcore中是lazy的。

spark移除缓存数据时，使用的是LRU算法，可以使用unpersist()来手动移除缓存数据。

unpersist()是立即执行(eager)的。

```
class StorageLevel private(
private var _useDisk: Boolean, //是否使用磁盘
private var _useMemory: Boolean, //是否使用内存
private var _useOffHeap: Boolean, //是否使用heap
private var _deserialized: Boolean, //是否使用序列化
private var _replication: Int = 1) //副本数量
在定义StorageLevel时，会将直接写明缓存的方式，是否进行序列化，副本数量，例如：
MEMORY_ONLY 只将数据进行缓存
MEMORY_ONLY_SER 将数据进行缓存并且进行序列化
MEMORY_ONLY_2 将数据缓存，并且副本书为2
```
|Storage Level|Meaning|
|----|----|
|MEMORY_ONLY|将java对象不进行序列化存储在jvm中，如果不能缓存的数据，在需要的时候重新计算|
|MEMORY_AND_DISK|将java对象不进行序列化存储在jvm中，如果内存中不实用的数据，存储在磁盘上，在使用的时候会读取出来|
|MEMORY_ONLY_SER|将序列化的数据存储在jvm中，如果使用比较好的序列化方式，速度更快，这样可以增加cpu的负载|
|MEMORY_AND_DISK_SER|将序列化对象存储在jvm中，如果内存中不实用的数据，存储在磁盘上，在使用的时候会读取出来|
|DISK_ONLY|将数据存储到磁盘上|
|MEMORY_ONLY_2|将数据缓存到jvm中，副本数量为2|
|OFF_HEAP|不建议使用，还在实验阶段|
在shuffle操作时，spark会自动缓存数据。

每一个RDD都可以使用不同的存储级别进行保存，从而允许持久化数据集在硬盘或内存作为序列化的Java对象，设置跨节点复制。

## 选择存储格式
在选择存储格式时，需要对cpu和memory之间做权衡。

> * 如果RDD可以使用默认的存储格式，就直接使用默认存储格式。
> * 如果使用默认存储格式不行的化，可以使用MEMORY_ONLY_SER，并且选择比较好的序列化方式，这样就可以更高效、更快。
> * 尽可能不要使用磁盘的存储格式。
> * 如果需要快容错恢复，使用有副本的存储格式。

## Shared Variables

Spark提供了两种共享变量的方式：broadcast variables(广播变量) 和 accumulators(累加器).

### Broadcast Variables
Broadcast Variables是允许程序员保存只读(read-only)变量缓存在每一个机器上(作用于executor上)而不是将副本拷贝到task中。
广播变量数据量不能太大，否则会造成oom。

mapJoin就是broadcastJoin。

driver会发起广播变量，发送到每个节点上。

如果数据量大于20KB，就需要调优啦。
```
def commonJoin(sc:SparkContext): Unit ={
val data=sc.parallelize(Array(("1","zhangsan"),("2","lisi"),("27","xiaoyao"))).map(x=>(x._1,x))
val fll=sc.parallelize(Array(("27","beijing","30"),("10","nanjing","28"))).map(x=>(x._1,x))
//后面的数字就会第几个元素,在进行join操作，需要将两个数据集都变成KV的形式。
data.join(fll).map(x=>{
  println(x._1+":"+x._2._1._2+":"+x._2._2._2)
}).collect().foreach(println)
}

def broadcastJoin(sc:SparkContext): Unit ={
//driver发起，以map回来，
val data=sc.parallelize(Array(("1","zhangsan"),("2","lisi"),("27","xiaoyao"))).collectAsMap()
val databroadcast=sc.broadcast(data)
//broadcast是kv形式
val fll=sc.parallelize(Array(("27","beijing","30"),("10","nanjing","28")))
  .map(x=>(x._1,x))

/**
  * yeild这个关键字总结：
  * 1.for循环的每次迭代，yield产生的每一个值，都会记录在循环中。
  * 2.循环结束之后，会返回所有yield数据的集合。
  * 3.返回的集合类型与迭代的类型相同。
  */
fll.mapPartitions(partition=>{
  val datastudent=databroadcast.value //获取广播里面的内容
  for ((key,value) <- partition if (datastudent.contains(key)))
    //yield的主要作用是记住每次迭代中的有关值，并逐一存入到一个数组中
    //多个阶段需要相同数据或者以反序列化形式存储
    yield (key,datastudent.get(key).getOrElse(""),value._2)
}).collect().foreach(println)
}
```
### Accumulators

Accumulators是变量只能做加法操作。在WebUI的Task选项中可以看到。
如果是数字累加器，可以直接使用SparkContext.longAccumulator()或者SparkContext.doubleAccumulator()来获取。
>* 通过聚合操作将累加器的值从work传送到driver
>* 只有driver端可以获取到累加器的值，使用value方法。
>* 对于task，累加器是只写的
>* 跨worker计算错误的数量

变量可以通过关联操作进行添加：
>* 用于有效地实现并行计数器和总和
>* 只有驱动程序才能读取累加器的值，而不是任务


如果需要自定义累加器，需要实现AccumulatorV2，需要实现一下方法：reset将累加器的值重置为0，add用来执行加法，merge将两个相同类型的累加器的值相加。

自定义的累加器，结果类型与元素的类型可以不同。要保证每个任务只对累加器执行一次执行操作，在转换操作，如果重新执行任务，可以多次更新累加器。

累加器的操作也是lazy的。

```
val sparkConf=new SparkConf().setMaster("local[2]").setAppName("AccumulatorApp")
val sc=new SparkContext(sparkConf)
val count=sc.longAccumulator
sc.parallelize(Array(1,2,3,4)).collect().foreach(x=>count.add(x))
println(count.value)
```

## Spark on YARN
### client
流程：
>* 加载spark的版本、设置application的名字，使用--name来指定。
>* 加载sparkenv
>* 启动yarn的web服务
>* 启动spark UI
>* 添加相应的jar包
>* 连接到rm，申请资源，检查资源是否满足
>* 申请container，启动am
>* 将相应的资源打成包传上去
>* 申请executor来执行。
在哪里提交，就在哪里运行。

运行的过程中，driver需要将相应的文件发到executor，driver端不能结束进程，
driver需要与executor通信，如果一个失败，就会在另外一个节点上执行(有可能不在集群内)。

可以在控制台上看到log日志。网络消耗大，频繁交互。
### cluster
在集群中随机找一个节点来运行任务。运行在AM节点上。

可以退出程序，不能看到log日志，网络开销少(在集群内)

查看日志：yarn logs -applicationId id

## SequenceFile
```
var sparkConf=new SparkConf()
    .setAppName("SequenceFileApp")
    .setMaster("local[2]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.registerKryoClasses()
var sc=new SparkContext(sparkConf)
/**
  * sequenceFile()版本会将数据的类型通过WritableConverter转换为Writable,在填写K时，建议填写为BytesWritable
  * BytesWritable是一个字节序列被当作key或者value。
  * 在处理的过程中，第一个参数不用处理，丢掉。
  * 底层调用hadoopFile的方法
  */
var file=sc.sequenceFile[BytesWritable,String]("")
file.map(x=>x._2.split(",")).map(x=>(x(0),x(1))).foreach(println)
sc.stop()
```
## Data Serialization

CPU、MetWork、Bandwidth、Memory是Spark的瓶颈。序列化可以减少网络I/O、和内存的消耗。

序列化在提高分布式应用程序性能中占有举足轻重的地位。

Spark提供两种序列化方式：
> * Spark默认使用java序列化。通过实现Serializable接口(标识接口)。java序列化灵活但是非常慢，序列化之后体积更大。
> * Kryo 序列化:比java序列化速度更快，但是它不支持所有的序列化类型类型，需要实现注册序列化类。

使用conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")来初始化sparkconf和切换序列化方式。
这个设置主要适用于节点之间的shuffle数据和将数据序列化磁盘上。
Kryo序列化在使用时，需要注册，这种序列化方式适用于网络密集型应用程序。
在spark2.0.0之后，基本类型的默认序列化方式设置为kryo。

算子里面使用到的外部变量，需要经过网络传输，需要序列化减少体积，减少网络开销，占用内存更小。

在使用kryo序列化的方式时，如果不注册类，也可以使用，需要存储整个这个自定义的类，这种是浪费的方式。
```
var sparkConf=new SparkConf()
  .setAppName("SequenceFileApp")
  .setMaster("local[2]")
  //在编写shell时，可以使用 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(classOf[Student]))
var sc=new SparkContext(sparkConf)
val students=ListBuffer[Student]()
for (i<- 1 to 1000000){
  students.append(Student(i,"eduspark_"+i,"30"))
}
var studentRdd=sc.parallelize(students)
studentRdd.persist(StorageLevel.MEMORY_ONLY_SER)
studentRdd.count()
Thread.sleep(20000)
sc.stop()
//自定义类
case class Student(id:Int,name:String,age:String)
```
序列化使用场景：1.节点之间shuffule数据；2.序列化到磁盘(cache)；3.算子里面使用到外部变量，体积小，网络开销小

## Components

Spark应用程序在集群中运行为多个独立的进程，它与SparkContext对象组成driver program。

Spark Context可以连接到多种cluster manager(local,standalone,mesos,yarn),cluster manager在应用程序中用于分布资源。

运行步骤：

> * 1.连接到cluster manager，Spark得到在集群节点上的executor，这些executor可以进行计算和存储应用程序的数据。
> * 2.发送应用程序代码到executors。
> * 3.SparkContext发送task运行到executor中。

架构介绍：
> * 每个应用程序拥有自己的executor进程，在整个生命周期，executor一直存在，使用多线程来运行任务。应用程序之间都是隔离的，调度端(自己调度自己的task)和executor端(在不同的jvm中运行task)也是隔离的。
如果不把数据写入到外部存储系统，spark应用程序之间不能实现数据共享。
> * spark与底层的cluster manager无关。

一个Spark应用程序拥有多个job，一个job有多个stage组成，一个stage有多个task。
## Glossary
|Term|Meaning|
|----|---|
|Application|包含driver program(驱动程序)和executors(执行器)|
|Application jar|不包含hadoop和spark的依赖，在运行时添加进来|
|Driver program|执行应用的main方法和创建SparkContext对象的进程|
|Cluster manager|从集群中获取资源的外部服务，可插拔的，例如local、yarn、mesos、standalone|
|Deploy mode|driver跑在哪儿;cluster模式，在集群中启动driver进程；client模式，不在集群中启动driver进程|
|Worker node|在集群中运行应用程序代码的节点(nodemanager)|
|Executor|在worknode节点上启动应用程序的进程,或者说executor运行在container中，可以跨节点传输数据。每个程序有自己的executor|
|Task|发送到executor的一个运行单元|
|Job|一个job包含多个task，触发action操作就会产生job|
|Stage|job可以被分为多个task的集合，遇到shuffle就产生新的stage|
    Application=driver program+executors
        driver：是一个进程，main方法创建SparkContext
            client： driver运行在当前节点，不能退出程序直到结束，可以看到日志，网络消耗大，频繁交互。
            cluster：driver运行在am节点上，可以断开，看不到log，网络消耗小。查看日志：yarn logs -applicationId id
        executors: 是一个进程，执行task。
            运行在work node==nodemanager

磁盘空间占用大于90%，HDFS集群会处于不健康状态。