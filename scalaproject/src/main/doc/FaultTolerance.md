# Fault tolerance in Apache Spark
## 1.Spark RDD Fault Tolerance
###1.1Fault Tolerant Semantic
>* RDD是一个不可变的数据集，每一个RDD都能记录与其他RDD操作依赖关系。
>* 如果由于worker节点故障导致一些RDD数据丢失，可以按照RDD的依赖关系，重新计算。
>* 不管Spark集群发生什么错误，最终结果都是一样。

###1.2 数据恢复
两种数据需要恢复：
> * Data received and replicated:节点发生故障，仍然有一个备份在其他机器上。
> * Data received but buffered for replication:没有对数据进行复制，只能从源中再次获取。
###1.3 发生故障
两种必须要考虑的失败情况：
> * Failure of a Worker Node：worker node中的executor失败，数据丢失。
> * Failure of the Driver Node：driver node中应用程序失败，数据丢失。
##2.Fault Tolerance with Receiver-based sources
对于基于接收器的数据源，是针对失败场景和接收器的类型来进行容错。两种类型的接收器：
>* Reliable Receiver:接收到数据，然后给一个回执,如果发生故障，重启接收器之后，数据源会再次发生数据。
>* Unreliable Receiver:接收到数据，不发回执，如果driver端或者executor端失败，将会丢失数据。
    
如果worker节点故障，Reliable Receiver不会丢失数据，Unreliable Receiver会丢失数据。对于Unreliable Receiver，接收到的数据但未复制的数据就会丢失。

##3.Spark Streaming write ahead logs
如果driver节点故障，存储在内存中的数据(接收到的和复制的数据)都会丢失。
Spark 2.1引入write ahead logs，将收到的数据存储起来进行容错。接收到的数据在streaming处理之前，都会存储到write ahead logs。
write ahead logs可以存储在文件或者数据库。在处理的过程中，系统故障可以按照write ahead logs恢复数据。

|部署场景|worker故障|driver故障|
|----|-----|---|
|Spark 1.1 or earlier|使用unreliable receiver时，<br>会丢失缓冲区的数据|使用unreliable receiver时，<br>会丢失缓冲区的数据|
|Spark 1.2 or later without <br>write ahead logs|使用reliable receiver时，不会丢失数据，<br>At-least-once语义|所有receiver丢失过去的数据，<br>Undefined语义|
|Spark 1.2 or later with <br>write ahead logs|使用reliable receiver时，<br>不会丢失数据，<br>At-least-once语义|使用reliable receiver时，<br>不会丢失数据，<br>At-least-once语义|