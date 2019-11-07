# Spark Streaming
## Spark Streaming介绍
Spark Streaming是在Spark Core的基础上进行扩展，可实现对实时数据的扩展、高吞吐量、容错性处理。

Spark Streaming的数据源：Kafka、Flume、HDFS/S3、Kinesis、Twitter。

Spark Streaming写入的地址：HDFS、DataSource、DashBoard。

Spark Streaming的工作方式是流，将数据接收到之后，分成批处理(不是实时)，以批处理为主，使用微批处理来解决实时问题。

Flink以stream为主,来解决批处理问题。

Spark Streaming将持续的数据流抽象为离散数据或者DStream。DStream是一连串的RDD。

可以通过Spark Streaming的实例创建Spark Context对象：ssc.sparkContext。

在间隔的时候是基于应用程序延迟的要求(处理完数据需要的时间)和资源可用的情况(在设置的时间范围内，可以处理完数据需要的资源)。

StreamingContext创建之后的操作流程：

> * 创建输入流(input DStream)
> * 通过转换操作计算输入流和将处理完的数据作为一个DStream输出出去
> * 使用streamingContext.start来开始接收和处理数据
> * streamingContext.awaitTermination()停下来等来数据的到来
> * 可以通过streamingContext.stop()停止整个流程

重点：

> * StreamingContext启动之后，不能添加新处理逻辑，加了也没有用
> * StreamingContext不具有重启的功能
> * 在同一个JVM中，同一时刻只能存在一个StreamingContext

## DStream
DStream表示连续的数据流，可以是从源数据中获取输入流，或者是通过转换操作处理的数据流。DStream是连续RDD的集合，DStream会将操作转换为RDD的操作。

DStream的内部属性：
> * DStream具有依赖性：后面的DStream依赖于之前的DStream
> * 固定的时间间隔会生成一个RDD
> * 处理数据之后，产生相应的RDD

简单实例代码(需要开启netcat)：
```
def socketStream(): Unit ={
/**
  *  准备工作
  *  1.不要将master硬编码
  *  2.将master通过参数传递过来(spark-submit)
  */
val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
/**
  * StreamingContext是SparkStreaming的入口点，它可以从多种输入源创建DStreams。
  * StreamingContext创建方式
  * 1.master url 和appName
  * 2.SparkConf
  * 3.已经存在的SparkContext
  * 构造器：
  * 1.主构造器：class StreamingContext private[streaming] (_sc: SparkContext,_cp: Checkpoint,_batchDur: Duration)
  * 2.副主构造器：
  * 2.1 def this(sparkContext: SparkContext, batchDuration: Duration) //SparkContext必须已经存在
  * 2.2 def this(conf: SparkConf, batchDuration: Duration)   //通过SparkConf创建一个新的SparkContext
  * Duration默认的单位为毫秒。
  * 副主构造器在后面都是调用主构造器来创建对象，其他的副主构造器不怎么用
  * 如果使用spark-shell操作时，默认情况下在同一个jvm中只能有一个SparkContext，因为使用2.2构造StreamingContext时会创建出来一个新的SparkContext会报错
  * 如果想要使用Sparkcontext来创建StreamingContext时,可以 set spark.driver.allowMultipleContexts = true
  * 可以通过conf来获取外面传过来的参数conf.get("spark.driver.allowMultipleContexts")
  * 可以通过ssc获取参数：ssc.sparkContext.getConf.get("spark.driver.allowMultipleContexts")
  * 不建议在spark-shell中使用2.1的构造器。
  *
  */
val ssc=new StreamingContext(conf,Seconds(1))
/**
  * 业务处理逻辑
  * socketTextStream 接收网络流(指定hostname:port)，使用UTF-8编码，使用\n作为行分隔符，默认的存储格式为MEMORY_AND_DISK_SER_2，返回值为ReceiverInputDStream
  * socketStream 接收网络流(指定hostname:port),需要指定编码格式、分隔符、存储格式，返回值为ReceiverInputDStream，使用的不多
  * rawSocketStream 接收网络流(指定hostname:port),接收序列化数据，不需要反序列化，默认的存储格式为MEMORY_AND_DISK_SER_2，这是最高效的接收数据的方式，返回值为ReceiverInputDStream
  * RawInputDStream extends ReceiverInputDStream extends InputDStream extends DStream
  * 注意：如果在spark-shell操作时，需要先启动netcat。
  */
val lines=ssc.socketTextStream("localhost",9999)
val words=lines.flatMap(_.split(" "))
val pairs=words.map(word=>(word,1))
val wordCound=pairs.reduceByKey(_+_)
//简写方式为
//var result=lines.flatMap(_.split(" ")).map(word=>(word,1)).reduceByKey(_+_)
/**
  * print()是将数据输出出来，相当于sparksql中的show或者说是一个action操作，或者说是一个output，默认输出10个
  */
wordCound.print()
//开始streaming
ssc.start()
ssc.awaitTermination()
}
```

## Input DStream和Receiver
SparkStreaming支持两种内嵌的数据源
> * 基本的源：可以直接通过Spark Context API来处理，例如：文件系统和socker连接。
> * 高级源：kafka、Flume、Kinesis，需要有相关的依赖。
如果想要从不同的源里面并行接收数据，需要创建多个输入DStream，设置相应的资源需要根据需求来定。
必须要保证收集数据的core数量>receiver的core数量。
除了filestream没有inputstream，其他都有inputstream。

重点：
> * master要设置多core,例如setMaster("local[2]"), local[n]中n>receiver的数量
> * 在集群中，收集数据的core数量>receiver的core数量

## Basic Sources

### File Streams
使用StreamingContext.fileSystem[KeyClass,ValueClass,InputFormatClass]可以从兼容HDFS的文件系统中读取数据。
文件流不需要receiver就是不占用core。

如何监控文件的目录：
> * 设置监控目录，该目录下的文件都能被发现和处理
> * 监控目录下文件都是同一格式
> * 监控文件是基于修改时间而不是创建时间
> * 如果文件被处理一次后，在当前的interval时间范围内修改文件之后，不会再次处理
> * 如果应用程序重新启动之后，不会处理监控文件夹存放的之前的文件(不对文件修改的前提下)

```
def fileStream(): Unit ={
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc=new StreamingContext(conf,Seconds(10))
    val dataDirectory:String="file:///Users/renren/Downloads/test"
    /**
      * fileStream创建一个输入流来监控hadoop兼容的文件系统产生的新文件，并且读取数据
      * 监控移动(move)到当前文件夹的文件,会疏略隐藏文件,需要在当前文件夹创建一个新的文件
      * 只读取在应用程序启动之后修改的文件和创建的文件(如果使用put操作，数据还没有写完，这个时间段就结束啦)
      * textFileStream读取文本文件，key是LongWritable，value是Text。
      * textFileStream底层调用的是
      * fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
      *   上面的_._2是内容，_._1是offset
      * 底层实现为FileInputDStream。在处理批次中，为了监控产生新的数据和新创建的文件，
      * FileInputDStream记录了上一批次处理的文件信息，并保留一段时间，在记住文件之前的修改，都会被丢弃。
      * 并对监控的文件做了如下假设：
      * 1.文件时钟与运行程序的时钟一致。
      * 2.如果这个文件在监控目录可以看到的，并且在remeber windows内可以看到，否则不会处理这个文件。
      * 3.如果文件可见，没有更新修改时间。在文件中追加数据，处理语义没有定义。
      *
      */
    val file=ssc.textFileStream(dataDirectory)
    val words=file.flatMap(x=>x.split(" "))
    //第一种方式
    //val wordCount=words.map(x=>(x,1)).reduceByKey(_+_)
    //第二种方式
    words.foreachRDD(rdd=>{
      //获取一个SparkSession
      val spark=SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      //或者使用单例模式来获取
      //val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordDataFrame=rdd.map(w => Record(w)).toDF()
      wordDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }
  //创建单例模式的sparksession(懒加载模式)
  object SparkSessionSingleton {
  
    @transient  private var instance: SparkSession = _
  
    def getInstance(sparkConf: SparkConf): SparkSession = {
      if (instance == null) {
        instance = SparkSession
          .builder
          .config(sparkConf)
          .getOrCreate()
      }
      instance
    }
  }
```
在处理文件时，如果过了处理时间，就会丢失数据，可以通过重命名文件移动到监控目录下，就可以处理了。

### Queue of RDDs as a Stream

将输入流放入到队列中，像其他流一样处理。

## Advance Sources:SparkStream与Kafka的结合
在Spark2.4之前，Kafka、Kinesis和Flume之前在python版本中不可用。

Spark2.4兼容Kafka broker0.8.2.1以及更高的版本。

从Kafka中接收数据有两种方式：使用Receiver和Kafka高级API、新的方法不用receiver。

### Receiver-based Approach

Receiver必须要实现Kafka consumer的高级API。从Kafka中接收到数据之后，存储到Spark executors，然后使用Spark Streaming来启动任务处理数据。

使用默认的配置，在应用程序失败时会丢失数据，可以通过开启Write-Ahead Logs来方式数据丢失。这种方式会将接收的数据异步保存到分布式文件系统的write-ahead logs 中。

1.导入依赖
导入相关依赖：
```
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>
```
开启zookeeper和Kafka。

2.编写程序
```
def kafkaStream(): Unit ={
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc=new StreamingContext(conf,Seconds(5))
    val topic="test"
    val numberPartitions=1
    val zkQuorm="localhost:2181"
    val groupId="tes"
    //分区与并行度没有关系
    val topics = topic.split(",").map((_,numberPartitions)).toMap
    val messages = KafkaUtils.createStream(ssc, zkQuorm, groupId, topics)
    messages.map(_._2) // 取出value
      .flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
      .print()
    ssc.start()
    ssc.awaitTermination()
  }
```
重点：
> * topic的分区与RDD的分区没有关系。增加topic分区数量只会增加处理topic的线程数，不会增加并行度。
> * 不同的分组/topic创建多个Kafka输入流来增加并行度。
> * 输入流的存储格式设置为StorageLevel.MEMORY_AND_DISK_SER。设置为多个副本没有意义，如果程序挂了，数据就会丢失。

3.部署

spark-core_2.11和spark-streaming_2.11标记为provided的依赖，在使用的时候，将Kafka相关的依赖加进去。
```
./bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.0
```
### Direct Approach

从Spark1.3之后可以没有receiver的方式来保证端到端的数据。在Kakfa0.10只有这个方式。

这种方式不需要receiver，每个批次都会定义offset的范围，会定期查询topic+partition的最新offset。

当启动处理程序时，Kafka comsumer API用于读取定义offset的范围。

这种方式的优点：
> * 将并行简化：不需要创建多个输入流并且和聚合输入流。Kafka partitions的数量与RDD的数量是1:1的关系。
> * 高效性：Receiver-based Approach方式需要写log:Kafka、Write-Ahead Log。Direct Approach不需要写log。如果保留期时间足够，可以获取kafka数据。
> * 真正一次语义：Receiver-based Approach将消费offset存储到Zookeeper。第一种读取两次，是因为Spark Streaming接收到的可靠数据和Zookeeper追踪的offset不一致。
Direct Approach使用checkpoint来追踪offset(消除不一致，保证处理一次)。为了保证数据只被处理一次，将数据写入到外部，保存数据的结果和offset。

使用步骤：

1.添加依赖：
```
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-streaming-kafka-0-8_2.11</artifactId>
  <version>${spark.version}</version>
</dependency>
```

2.编写程序

```
def directKafka(): Unit ={
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")
    val ssc = new StreamingContext(conf, Seconds(10))
    val topic = "test"
    /**
    *  必须要明确broker的地址：metadata.broker.list或者bootstrap.servers
    */
    val kafkaParams = Map[String, String]("metadata.broker.list"->"localhost:9092")
    val topics = topic.split(",").toSet
    /**
      * 创建输入流从kafka中拉取数据，不实用receiver。保证数据只被处理一次
      * 重点：
      * 1.没有receiver。
      * 2.offset:offset不存储在zookeeper中，自己更新offset。
      * 3.故障恢复：开启checkpoint来存储offset
      * 4.端到端语义：数据只被处理一次，输出不一定是只有一次，需要使用幂等操作或者使用事务来保证只输出一次
      */
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams, topics)

    messages.map(_._2) // 取出value
      .flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
      .print()

    ssc.start()
    ssc.awaitTermination()
  }
```
在上面的代码中，必须要设置broker的地址，参数为metadata.broker.list或者bootstrap.servers。默认情况下是读取每个分区的最新offset。
如果设置auto.offset.reset=smallest，就会从最小的offset开始消费。

可以从任意offset来读取数据，可以将offset来存储数据。

HasOffsetRanges的类型转换只能在第一次调用directKafkaStream方法时，才会成功。可以使用tranform()替代foreachRDD()来访问offset。

这种方法没有receiver。需要使用"spark.streaming.kafka.*"来配置," spark.streaming.kafka.maxRatePerPartition"设置读取每个partition的最大百分比。

管理offset代码：
```
def offsetManage(): Unit ={
    // 准备工作
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")
    /**
      * 修改代码之后不能用(checkpoint)。
      * 小文件比较多
      */
    val checkpointPath = "hdfs://hadoop000:8020/offset_xiaoyao/checkpoint"
    val topic = "test"
    val interval = 10
    val kafkaParams = Map[String, String]("metadata.broker.list"->"hadoop000:9092","auto.offset.reset"->"smallest")
    val topics = topic.split(",").toSet
    def function2CreateStreamingContext():StreamingContext = {
      val ssc = new StreamingContext(conf, Seconds(10))
      val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams, topics)
      ssc.checkpoint(checkpointPath)
      messages.checkpoint(Duration(8*10.toInt*1000))

      messages.foreachRDD(rdd => {
        if(!rdd.isEmpty()){
          println("---count of the data is ：---" + rdd.count())
        }
      })
      ssc
    }

    val ssc = StreamingContext.getOrCreate(checkpointPath, function2CreateStreamingContext)
    ssc.start()
    ssc.awaitTermination()
  }
```
将offset存储到mysql中，进行处理
```
def main(args: Array[String]) {
    // 准备工作
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectKafkaApp")
    /**
      * 加载数据库配置信息
      */
      DBs.setup()
    val fromOffsets = DB.readOnly{implicit session => {
        sql"select * from offsets_storage".map(rs =>{
          (TopicAndPartition(rs.string("topic"),rs.int("partition")), rs.long("offset"))
        }).list().apply()
      }}.toMap

    val topic = ValueUtils.getStringValue("kafka.topics")
    val interval = 10
    //val kafkaParams = Map[String, String]("metadata.broker.list"->"localhost:9092","auto.offset.reset"->"smallest")

    val kafkaParams = Map(
      "metadata.broker.list" -> ValueUtils.getStringValue("metadata.broker.list"),
      "auto.offset.reset" -> ValueUtils.getStringValue("auto.offset.reset"),
      "group.id" -> ValueUtils.getStringValue("group.id")
    )
    val topics = topic.split(",").toSet
    val ssc = new StreamingContext(conf, Seconds(10))
      //TODO... 去MySQL里面获取到topic对应的partition的offset
    val messages = if(fromOffsets.size == 0) { // 从头消费
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams, topics)
    } else { // 从指定的位置开始消费
      //val fromOffsets = Map[TopicAndPartition, Long]()
      val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
      KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc, kafkaParams, fromOffsets,messageHandler)
    }
    messages.foreachRDD(rdd => {
      if(!rdd.isEmpty()){
        println("---the count of the data is ：---" + rdd.count())

        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        offsetRanges.foreach(x => {
          println(s"--${x.topic}---${x.partition}---${x.fromOffset}---${x.untilOffset}---")

          DB.autoCommit{
            implicit session =>
              sql"replace into offsets_storage(topic,groupid,partition,offset) values(?,?,?,?)"
                .bind(x.topic,ValueUtils.getStringValue("group.id"),x.partition, x.untilOffset)
                  .update().apply()
          }
        })
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
```
### Receiver Reliability

Receiver需要一直在工作来接收数据(job0,这个是一直存在，可以在sparkUI页面看到)。
> * Reliable Receiver:需要发送ack
> * Unreliable Receiver:不需要发送ack

## Transformations on DStreams

输入的DStream可以通过transformations对数据操作，可以修改数据产生一个新的DStream。

|Transformation|Meaning|
|---|---|
|map|通过源DStream处理每一个元素获取一个新的DStream，对元素操作|
|transform|对DStream的每一个RDD进行操作，每个RDD或产生一个新的RDD，构成一个新的DStream，对RDD进行操作|
|flatMap|将输入的元素映射为0个或者多个元素|
|filter|值返回需要的元素或者说是经过处理之后范围之为true的元素组成的DStream|
|repartition|重新分区，变更分区数|
|union|源DStream与其他DStream进行union操作合成的一个DStream|
|count|返回每个RDD中元素的个数，组成一个新的DStream|
|reduce|两个具有kv的DStream，进行join操作，返回用(K,Seq[V],Seq[W])组成的DStream|
|countByValue|计算key的个数|
|reduceByKey|对key进行聚合操作，默认情况下，使用并行的数量，可以通过spark.default.parallelism来设置并行数量|
|join|对key进行join操作，形成(k,(v,w))|
|cogroup|执行join操作，将数据变成(k,Seq[V],Seq[W])|
|updateStateByKey|记录key的状态，根据key来更新数据|

### UpdateStateByKey
UpdateStateByKey操作维护了key的状态，根据key来更新后面的数据。

无状态的方式：只处理当前批次的数据。

有状态的方式：该批次的数据和以前批次的数据是需要“累加”的

操作步骤：
> * 定义状态：状态可以是任意状态。
> * 定义更新状态的方法：定义一个函数，使用之前的状态和新的数据来更新数据。

相关代码：
```
def socketStream(): Unit ={
    //做一个开关
    //将需要过滤的数据写到数据库中
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc=new StreamingContext(conf,Seconds(5))
    //如果是有状态的操作，需要要设置checkpint
    ssc.checkpoint(".")
    val lines=ssc.socketTextStream("localhost",9999)
    val result=lines.flatMap(_.split(",")).map(x=>(x,1))
    val state=result.updateStateByKey(updateFunction)
    state.print()
    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }

  def updateFunction(currentValues:Seq[Int],preValues:Option[Int]): Option[Int] ={
    val curr=currentValues.sum
    val prev=preValues.getOrElse(0)
    Some(curr+prev)
  }
```

### Transform Operations
```
 def blackList(): Unit ={
    val sparkConf=new SparkConf().setMaster("local[2]").setAppName("BlackListApp")
    val sc=new SparkContext(sparkConf)
    //1.名字，2.info
    val input=new ListBuffer[(String,String)]
    input.append(("yishengxiaoyao","yishengxiaoyao info"))
    input.append(("xiaoyaoyisheng","xiaoyaoyisheng info"))
    input.append(("xiaoyaoshan","xiaoyaoshan info"))
    input.append(("xiaoyao","xiaoyao info"))
    //将数据并行变成RDD
    val inputRDD=sc.parallelize(input)
    //黑名单：1.name,2.false
    val blackTuple=new ListBuffer[(String,Boolean)]
    blackTuple.append(("yishengxiaoyao",true))
    blackTuple.append(("xiaoyao",true))
    val blackRdd=sc.parallelize(blackTuple)
    //使用左外连接，如果后面没有数据，设置为null
    inputRDD.leftOuterJoin(blackRdd).filter(x=>{
      x._2._2.getOrElse(false)!=true
    }).map(_._2._1).collect().foreach(println)
    sc.stop()
  }
```

### Window Operations

窗口操作有两个重要参数：窗口大小、滑动间隔。

窗口大小和滑动间隔必须是间隔的整数倍(The window duration of windowed DStream/The slide duration of windowed DStream must be a multiple of the slide duration of parent DStream.)。

window length:窗口的持续时间。
sliding interval:执行窗口操作的间隔。

|Transform|Meaning|
|----|----|
|window|基于windows批处理的DStream，返回一个DStream|
|countByWindow|返回当前Stream中元素数量|
|reduceByWindow|在当前批次中，通过聚合操作，返回一个单元素的流|
|reduceByKeyAndWindow|spark.default.parallelism设置并行数，并行执行reduceByWindow操作|
|countByValueAndWindow|并行执行countByWindow|

```
def socketStream(): Unit ={
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc=new StreamingContext(conf,Seconds(5))
    val lines=ssc.socketTextStream("localhost",9999)
    val results=lines.flatMap(x=>x.split(","))
          .map((_,1))
          .reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),Seconds(10),Seconds(5)).print()
    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }
```

## Design Patterns for using foreachRDD

需要记录的重点：
> * DStream在执行时时Lazy，想要输出数据，需要有一个action操作
> * 默认情况下，输出操作在某一时刻执行一次。

```
def socketStream(): Unit ={
    //做一个开关
    //将需要过滤的数据写到数据库中
    val conf=new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc=new StreamingContext(conf,Seconds(5))
    val lines=ssc.socketTextStream("localhost",9999)
    val results=lines.flatMap(x=>x.split(",")).map((_,1)).reduceByKey(_+_)
    //第一种写法,基于数据连接
    results.foreachRDD(rdd=>{
      //在executor端创建connection，否则会报没有序列化的错误，因为需要跨机器传输,需要使用第二种写法
      val connection=createConnection() //在driver端执行
      rdd.foreach(pair=>{
        val sql=s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
        connection.createStatement().execute(sql) //执行在worker端
      })
      connection.close()
    })
    //第二种写法，
    results.foreachRDD(rdd=>{
      rdd.foreach(pair=>{
        //RDD中的每个元素都要创建连接，每次创建连接和销毁连接都需要时间，使用rdd.foreachPartition创建连接
        val connection=createConnection()
        val sql=s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
        connection.createStatement().execute(sql) //执行在worker端
        connection.close()
      })
    })
    //第三种写法,基于partition的连接
    results.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //每个partition创建一个连接
        val connection=createConnection()
        partition.foreach(pair=>{
          val sql=s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })
    //编写一个连接池
    //第四种写法，在第三种方式中进行优化，基于静态连接
    results.foreachRDD(rdd=>{
      rdd.foreachPartition(partition=>{
        //创建连接池
        val connection=createConnection()
        partition.foreach(pair=>{
          val sql=s"insert into wc(word,count) values('${pair._1}','${pair._2}')"
          connection.createStatement().execute(sql)
        })
        connection.close()
      })
    })

    //开始streaming
    ssc.start()
    ssc.awaitTermination()
  }

  def createConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://localhost:3306/test","root","123456")
  }
```

## Output Operations on DStreams

OutputStream是将数据输出到外部系统。

|Output Operation|Meaning|
|----|-----|
|print()|输出前10条记录|
|saveAsTextFiles|将DStream的内容写到文本文件，会产生大的量的小文件，不怎么用|
|saveAsObjectFiles|将DStream的内容进行序列化写入到SequenceFile，不怎么用|
|saveAsHadoopFiles|将DStream的内容写到HDFS文件，不怎么用|
|foreachRDD|将DStream转换为RDD，将文件写入到外部，这是最通用的方法，在driver端处理|

## DataFrame and SQL Operations

使用DataFrame和SQL来操作数据流，使用StreamingContext使用的SparkContext来创建SparkSession。
另外，这样操作可以在driver端出现故障是可以重启。这是通过创建延迟实例话的SparkSession对象(SparkSession是一个单例类)。
关于代码，可以参考上面的fileStream方法的代码。

## Fault-tolerance Semantics

RDD的容错语义：
> * RDD是一个不可变的、可重复计算的分布式数据集。每个RDD都会记录在容错数据集上创建线性操作。
> * 节点挂拉，使用线性操作来执行原先RDD。
> * 不管集群出现什么问题，执行相同操作，结果都应该是一样的。

为了解决没有receiver的处理方式，在节点上默认的存储为MEMORY_AND_DISK_SER_2.

两种数据需要恢复：
> * Data received and replicated:节点发生故障，仍然有一个备份在其他机器上。
> * Data received but buffered for replication:没有对数据进行复制，只能从源中再次获取。

两种必须要考虑的失败情况：
> * Failure of a Worker Node：worker node中的executor失败，数据丢失。
> * Failure of the Driver Node：driver node中应用程序失败，数据丢失。

###定义
> * 最多一次：数据最多只能被处理一次。
> * 至少一次：数据会被处理一次或者多次。
> * 真正一次：数据只会被处理一次，不会丢失数据、不会被处理多次。最好的方式。

### Basic Semantics
处理流的三个步骤：
>* 接收数据：使用Receiver或者其他的接收器从源来接收数据。
>* 转换数据：使用DStream和RDD的转换操作来实现流的转换。
>* 输出数据：将数据写入到外部文件系统。

为了保证端到端的只读一次，每一个步骤都要保证只读一次。理解一下Spark Streaming上下文的语义：
>* 接收数据：不同的数据源提供不同的保证
>* 转换数据：通过RDD来接收的数据，可以保证只接收一次(RDD的容错机制)。
>* 输出数据：输出操作默认为至少一次，这个依赖于输出的类型和输出到的文件系统。

### Semantics of Received Data

#### With Files

如果从容错文件系统(例如:HDFS)中读取数据，Spark Streaming可以从失败中恢复并且处理全部数据。

#### With Receiver-based Sources

两种receiver：
>* Reliable Receiver:接收到数据，然后给一个回执。
>* Unreliable Receiver:接收到数据，不发回执，如果driver端或者executor端失败，将会丢失数据。

|Deployment Scenario|Worker Failure|Driver Failure|
|-----|-----|-----|
|Spark 1.1 or earlier, OR<br>Spark 1.2 or later without write-ahead logs|不可靠Reciever丢失缓冲区数据<br>可靠Receiver不丢失数据<br>至少一次语义|不可靠Reciever丢失缓冲区数据<br>所有Receiver丢失过去的数据<br>未定以语义|
|Spark 1.2 or later with write-ahead logs|可靠Reciver的零数据丢失<br>至少一次语义|文件和可靠Receiver零数据丢失<br>至少一次语义|
#### Semantics of output operations
输出操作是至少一次语义,为了保证只有一次语义,有以下方式：
>* 幂级更新：多次写出相同的数据
>* 事务更新：保证所有的更新都是事务，需要做以下操作：使用批次时间和分区索引作为唯一标识符;使用唯一标识符来提交更新，如果更新接收到确认，然后跳过这个提交。