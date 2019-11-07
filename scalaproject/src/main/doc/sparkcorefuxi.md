#Spark Core

每一个Spark Application是由driver程序(执行用户的main功能)和在集群中运行多种并行操作。

RDD是Spark的最主要的抽象,RDD是一个元素集合，将元素分散到集群中的每个节点，可以并行操作。

创建RDD的方法:1.通过文件创建(类Hadoop文件系统);2.其他RDD转换过来。可以通过持久化(到内存中)，在并行操作可以高效重用。
RDD可以从失败中自动修复。

Spark的第二个抽象是共享变量，这个共享变量可以在并行操作中使用。默认情况下，将Spark任务作为一个任务集合在不同节点上并行跑，
将在方法中运用到的每一个变量拷贝到集群中的每一个节点。Spark支持两种共享变量:1.广播变量:在每个节点上存储在内存中;2.累加器:只进行累加操作。


SparkContext是Spark功能的主要进入点，SparkContext用于连接到Spark集群，也可以在集群中创建RDD、累加器和广播变量。
每一个JVM中只能有一个SparkContext。在创建新的SparkContext对象时，必须要将之前的SparkContext对象关闭。

SparkConf用于Spark应用程序的配置，使用KV键值对。加载的配置都是以spark为开头(例如spark.*)。如果单元测试，new SparkConf(false)跳过加载外部环境。
使用ConcurrentHashMap来存储变量。

Spark-shell=>Spark-submit=>Spark-class(加载配置load-spark-env.sh=>conf/spark-env.sh)。

创建RDD的方式:1.在驱动程序中在现有的集合中并行;2.引用外部存储系统中的数据集。

```
def parallelize[T: ClassTag](
      seq: Seq[T],
      numSlices: Int = defaultParallelism): RDD[T] = withScope {
    assertNotStopped()
    new ParallelCollectionRDD[T](this, seq, numSlices, Map[Int, Seq[String]]())
}
//可以设置并行度:每个分区就是一个任务。在集群中，Spark尝试自动设置分区数量。
```

Spark在读取文件时需要注意的点:
1.如果使用本地文件系统，文件需要在所有节点上相同的路径中存在。或者将文件复制到所有节点上或者使用在网络中共享的文件.<br>
2.所有基于文件的输入方式，可以读取整个文件系统、压缩文件和文件正则表达式的文件路径。<br>
3.textFile第二个参数可以设置读取文件的分区数。<br>
除了文本文件，Spark还支持其他数据格式:
1.SparkContext.wholeTextFiles用于读取大量小文件，返回的数据为(filename,content)键值对。textFile会返回每一行数据。
数据本地性决定分区数，在某种情况，有很少的分区数。wholeTextFiles可以设置最小的分区数。<br>
2.对于SequenceFiles，使用 sequenceFile[K, V]方法，K表示key的类型，V表示value的类型，这些都是Writable的子类。


RDD操作:
1.transformation:从一个现有的RDD中创建的新数据集;2.action:在计算之后，将结果返回给驱动程序。

所有的transformation都是lazy的，在碰到action的时候，才会触发之前的transformation操作。默认情况下，在碰到action时，就会重新计算之前的tranformation操作。<br>
为了提高效率，可以对数据进行持久化。

cache=>persist()=>persiste(StorageLevel.MEMORY) 这个操作是lazy的，在Spark SQL中是eager的。

理解闭包:
在集群中运行代码，理解变量和方法的范围和生命周期。<br>
Spark在运行任务的时候，将任务分为多个task,每个task由一个executor执行，在运行任务之前，Spark优先运行闭包。<br>
在运行RDD的时候，闭包的变量和方法都是在executor中可见。这个比较是可序列化并且发送到每一个executor。<br>
闭包里面的变量将副本发送到每一个executor,因此，在foreach中函数中引用计数器，它不再是驱动程序节点上的计数器。<br>
驱动节点上面的计数器是存储在内存上，不能在其他的executors上可见。executors只对序列化的闭包中的副本。<br>
这种情况下最好使用计数器。

在集群模式中，由Executor执行输出写入的是Executor的stdout，而不是driver上的那个stdout，所以driver的stdout不会显示这些！要在driver中打印所有元素，可以使用该collect()方法首先将RDD数据带到driver节点：rdd.collect().foreach(println)。但这可能会导致driver程序内存不足，因为collect()会将整个RDD数据提取到driver端; 如果您只需要打印RDD的一些元素，则更安全的方法是使用take()：rdd.take(100).foreach(println)。