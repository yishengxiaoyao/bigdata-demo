# RDD vs DataFrame vs Dataset
本篇文章主要是介绍RDD、DataFrame、Dataset这三者的不同
##1.RDD、DataFrame、Dataset介绍
### 1.1 RDD
RDD是Resilient Distributed Datasets的简写。它是一个只读的数据集合。RDD是Spark最基础的数据结构。
RDD允许开发者使用容错的形式在集群中使用内存计算，这样可以提高计算速度。
### 1.2 DataFrame
DataFrame是使用数据组成命名列，它是一个不可变的分布式数据集合。
### 1.3 Dataset
Dataset是基于DataFrame的一个扩展，它支持强类型、面向对象编程的接口。Dataset可以通过表达式和字段，使用Catalyst优化器来进行查询优化。
## 2.RDD vs DataFrame vs Dataset
### 2.1 什么时候发布
>* RDD在Spark 1.0发布的
>* DataFrame 是在Spark 1.3发布
>* Dataset 是在Spark 1.6发布的
### 2.2 数据格式
#### 2.2.1 RDD
RDD可以处理结构化或者非结构化数据。RDD不需要获取数据的源数据。
#### 2.2.2 DataFrame
DataFrame只使用结构化和半结构化数据。DataFrame允许Spark管理schema信息。
#### 2.2.3 Dataset
Dataset可以处理结构化或者非结构化数据，使用行或者行集合表示JVM对象中的数据。
### 2.3 不可变性和可互操作性
RDD包含的数据集合都是可分区的，分区数就是并行度。每个分区是数据的逻辑分区，数据是不可变的，并且可以通过其他RDD使用转换操作获取。
不可变性可以保证计算的一致性。可以通过toDF()方法来生成DataFrame。

转换为DataFrame之后，无法再次生成域对象。可以通过rdd方法生成RDD对象。

可以使用Dataset和DataFrame转换为Dataset。

### 2.4 编译时期类型安全

| |SQL|DataFrames|Datasets|
|----|----|----|-----|
|参数错误|运行时错误|编译时错误|编译时错误|
|分析错误|运行时错误|运行时错误|编译时错误|

### 2.5 优化
>* RDD:RDD中没有内置的优化引擎。当处理结构化数据，RDD不能使用Spark高级优化器。
>* DataFrame:使用catalyst优化器来进行优化。DataFrame在下面4个阶段使用catalyst转换框架:a)分析逻辑计划解析饮用;b)优化逻辑计算;c)物理计划；d)代码生成部分查询编译为Java字节码。
>* Dataset:使用优化器来优化执行计划。

### 2.6 序列化

RDD在写数据时，使用的时Java序列化，在序列化数据时和节点之间发送数据时，成本高。

在使用DataFrame时，可以将数据以二进制格式存储到堆外，如果知道schema信息时，可以直接处理堆外数据，在编数据时，不需要使用java序列化，Spark提供了一个Tungsten物理执行后端，它可以明确的管理内存并动态生成字节码来进行表达式评估。

在序列化数据时，Dataset有自己的api来实现表格数据与序列化数据的转换。Dataset使用Spark内部的Tungsten二进制格式来存储表格数据，Dataset只获取需要的数据。

### 2.7 垃圾回收
>* RDD:创建和销毁单个对象而导致垃圾收集的开销。
>* DataFrame:在构造单个对象时，避免垃圾收集成本。
>* Dataset: 不需要销毁对象，因为序列化使用tungsten进行，使用offheap数据序列化。

### 2.8 效率/内存使用

RDD在序列化数据时，效率低，花费了大量的时间。

DataFrame使用offheap内存可以减少花销，DataFrame动态生成字节代码，以便可以对该序列化数据执行许多操作。无需对小型操作进行反序列化。

Dataset无需对整个对象进行序列化。

### 2.9 schema信息

RDD需要手动定schema信息。

DataFrame可以根据数据自动发现schema信息。

Dataset可以自动发现文件的schema信息。

### 2.10 聚合

RDD在进行分组和聚合操作时，速度慢。

DataFrame提供的API，在大量数据集上执行聚合操作、分析操作速度很快。

Dataset在大量数据集上执行聚合操作，效率高。