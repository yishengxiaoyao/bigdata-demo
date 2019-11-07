package com.edu.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StreamApp {
  def main(args: Array[String]): Unit = {
    //socketStream()
    fileStream()
  }

  def fileStream(): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
    val ssc = new StreamingContext(conf, Seconds(10))
    val dataDirectory: String = "file:///Users/renren/Downloads/test"
    /**
      * fileStream创建一个输入流来监控hadoop兼容的文件系统产生的新文件，并且读取数据
      * 监控移动(move)到当前文件夹的文件,会疏略隐藏文件,需要在当前文件夹创建一个新的文件
      * 只读取在应用程序启动之后修改的文件和创建的文件(如果使用put操作，数据还没有写完，这个时间段就结束啦)
      * textFileStream读取文本文件，key是LongWritable，value是Text。
      * textFileStream底层调用的是
      * fileStream[LongWritable, Text, TextInputFormat](directory).map(_._2.toString)
      * 上面的_._2是内容，_._1是offset
      * 底层实现为FileInputDStream。在处理批次中，为了监控产生新的数据和新创建的文件，
      * FileInputDStream记录了上一批次处理的文件信息，并保留一段时间，在记住文件之前的修改，都会被丢弃。
      * 并对监控的文件做了如下假设：
      * 1.文件时钟与运行程序的时钟一致。
      * 2.如果这个文件在监控目录可以看到的，并且在remeber windows内可以看到，否则不会处理这个文件。
      * 3.如果文件可见，没有更新修改时间。在文件中追加数据，处理语义没有定义。
      *
      */
    val file = ssc.textFileStream(dataDirectory)
    val words = file.flatMap(x => x.split(" "))
    //第一种方式
    //val wordCount=words.map(x=>(x,1)).reduceByKey(_+_)
    //第二种方式
    words.foreachRDD(rdd => {
      //获取一个SparkSession
      val spark = SparkSession.builder().config(rdd.sparkContext.getConf).getOrCreate()
      //或者使用单例模式来获取
      //val spark=SparkSessionSingleton.getInstance(rdd.sparkContext.getConf)
      import spark.implicits._
      val wordDataFrame = rdd.map(w => Record(w)).toDF()
      wordDataFrame.createOrReplaceTempView("words")
      val wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def socketStream(): Unit = {
    /**
      * 准备工作
      *  1.不要将master硬编码
      *  2.将master通过参数传递过来(spark-submit)
      */
    val conf = new SparkConf().setMaster("local[2]").setAppName("StreamApp")
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
    val ssc = new StreamingContext(conf, Seconds(1))
    /**
      * 业务处理逻辑
      * socketTextStream 接收网络流(指定hostname:port)，使用UTF-8编码，使用\n作为行分隔符，默认的存储格式为MEMORY_AND_DISK_SER_2，返回值为ReceiverInputDStream
      * socketStream 接收网络流(指定hostname:port),需要指定编码格式、分隔符、存储格式，返回值为ReceiverInputDStream，使用的不多
      * rawSocketStream 接收网络流(指定hostname:port),接收序列化数据，不需要反序列化，默认的存储格式为MEMORY_AND_DISK_SER_2，这是最高效的接收数据的方式，返回值为ReceiverInputDStream
      * RawInputDStream extends ReceiverInputDStream extends InputDStream extends DStream
      * 注意：如果在spark-shell操作时，需要先启动netcat。
      */
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCound = pairs.reduceByKey(_ + _)
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
}

case class Record(word: String)

//创建单例模式的sparksession(懒加载模式)
object SparkSessionSingleton {

  @transient private var instance: SparkSession = _

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