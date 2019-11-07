#Flume相关
##Flume定义
Flume是一个分布式、可靠和可用的系统，可以有效的从多种数据源中收集、聚合和移动大量数据到集中式数据存储。
##Flume结构介绍
Flume是有Source、Channel、Sink三个组成部分。

Source:收集数据。

Channel:缓冲。

Sink:写数据到外部。
###1. Flume Source
####1.1 Avro Source
####1.2 Kafka Source
####1.3 Taildir Source
####1.11 HTTP Source
###2. Flume Sink
###3. Flume Channel
####3.1 Memory Channel
事件存储在内存队列中，并设置最大值。适用于高吞吐量的数据流，如果发生故障会丢失部分数据。

|属性|值|描述|
|---|---|---|
|type||设置为memory|
|capacity|100|存储在通道中事件的最大值|
|transactionCapacity|100|每次事务传输的事件数量|
|keep-alive|3|添加或者删除事件的超时事件|
|byteCapacityBufferPercentage|20|byteCapacity/通道中所有的事件的数据量|
|byteCapacity|内存中最大的字节数作为事件的总和，内存中的80%|


###4. Flume Channel Selectors
###5. Flume Sink Processors
###6. Event Serializers
###7. Flume Interceptors
###8. Flume Properties