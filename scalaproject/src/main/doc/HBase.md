# HBase架构介绍
文章翻译自[An In-Depth Look at the HBase Architecture](https://mapr.com/blog/in-depth-look-hbase-architecture/)
HBase的主从架构中有三种不同的类型的servers组成。
Region Servers主要负责数据读写。在访问数据时，client直接和RegionServers进行沟通。
HBase Master用于重新分配Region，执行DDL操作。
Zookeeper是HDFS的一部分，用于维护实时集群的状态。
RegionServer管理存储在DataNode上的数据。所有的HBase数据都是存储在HDFS上。
RegionServer和DataNode进行搭配，DataNode为RegionServer提供的数据启用数据本地行性。
HBase在执行写操作的，都是将数据放在本地，当region移动时，数据在压缩之前不是本地的。
NameNode维护组成文件的所有物理数据块的源数据信息。
## Regions
HBase表按照row keys水平分为"Regions"。一个Region包含区域的开始Key和结束key之间的所有key。key是regionid。
在节点上分配Region的节点为Region Server，以及管理数据的读写操作。一个Region Server可以管理1000个Region。
## HBase Master
HBase Master处理region的分配和DDL操作。
Master的职能：
>* 协调region server:1.在启动的时候，分配regions，在进行恢复时，重新分配区域或者负载均衡；2.架空所有的RegionServer实例。
>* 管理功能：创建、删除、更新表的接口。

## Zookeeper：协调者
HBase将Zookeeper作为一个协调者来维持集群中erver状态。
Zookeeper维护erver的活着并且可用，如果server挂掉，会发出通知。
Zookeeper用共识来保证共享状态。差不多3或者5太集群来完成共识。(Zookeeper的数量只能是单数。)

## 组建协作
Zookeeper用于协调分布式系统成员的共享信息。Region Server和处于活跃状态的HMaster使用session来连接到Zookeeper。
Zookeeper通过心跳来维护临时节点活跃的session。

每个RegionServer创建一个临时节点，HMaster监控这些临时节点来发现region server，监控临时节点来判断哪些节点挂啦。
HMaster争相创建临时节点，Zookeeper来决定那个第一个创建的，并且保证只有一个HMaster是活跃的，另外一个是standby。
活跃的HMaster往Zookeeper中发送心跳，standby的HMaster监听活跃HMaster是否挂啦。

如果region server或者活跃的HMaster不发送心跳，session就会过期并且协同的临时节点就会被删除。
更新的监听器会收到删除节点的通知。活跃的HMaster节点监听region server，并且修复挂啦的region server。
standBy的HMaster节点在监听到活跃的HMaster节点挂啦，standby会自动变成active的。

##HBase 读/写
HBase Catalog 表是Meta表，它包含了所有集群中节点的位置，Zookeeper存储Meta表的位置。

在HBase中，客户端第一次读或者写入数据时步骤：
>* 客户端从Zookeeper中获取Meta表所在的RegionServer。
>* 客户端查询Meta数据来获取读取数据所在的节点。客户端会将Meta数据缓存。
>* 从相应的RegionServer中获取Row。

再次进行读取数据时，可以从当前缓存中获取Meta表的位置和之前的读取的row keys。
随着时间的流失，当region发生移动后，会更新缓存数据。

## HBase Meta Table
Meta表是根据指定的key来找到相应的region。
>* Meta表是一个HBase表，Meta表维持系统中的所有region。
>* Meta表类似一个树(BTree)
>* Meta表的结构:a) Key:region开始的key,region id;b)Values:RegionServer

## RegionServer组件
RegionServer运行在DataNode上，拥有以下组件：
>* WAL:Write Ahead Log是分布式文件系统中的一个文件。这个WAL用来存储那些还没有持久化到永久存储的数据;如果在失败的时候，可以使用WAL来恢复数据。
>* BlockCache:读缓存。在内存中存储经常读取的数据。使用LRU的算法将数据清楚。
>* MemStore:写缓存。存储还没有写到磁盘的数据。在写入到磁盘之前，将数据进行排序。每个region，每个Column family都有一个MemStore。
>* Hfiles(在磁盘上排序的KeyValues)将行存储为磁盘上的排序KeyValues。

## HBase写步骤

当cliet发送put请求时，第一步是将数据写入到WAL中，这个WAL：
>* 将修改的内容添加到WAL文件后面存储在磁盘上。
>* 如果服务器崩溃可以使用WAL来恢复没有持久化的数据。

当数据写入到WAL之后，将数据放入到MemStore，然后将put请求的确认返回到客户端。

## HBase MemStore

MemStore将更新操作作为排序的KeyValue放入内存中，就和存储在HFile中的一样。每一个column family一个MemStore。更新按照column family排序。

## HBase Region Flush
当MemStore累积足够多的数据，这个排完序的集合写入HDFS上的一个HFile。
HBase中的每一个column family拥有多个Hfile，column family包含真正单元，或者KeyValue实例。
这些文件随着时间的推移而创建，作为KeyValue修改操作存储在MemStore中，然后作为文件刷到磁盘上。

这就是HBase中column family中数量限制的原因。每个column family都有一个MemStore，当一个MemStore写完之后，然后刷到文件中。
并且保存上次写入序列编号以至于这个系统现在持久化的位置。

最大序列号经过排序作为HFile中的meta field，然后映射结束和继续的位置。在region启动的时候，读取序列号，最大的序列号作为新编辑的序列号。

## HBase HFile
数据存储在HFile中，Hfile的key/value都是排序的。当MemStore累积到足够多的数据，整个排序的KeyValue集合写入到在HDFS上的新HFile。
这是按照数据写，速度快，避免了移动磁盘驱动头。

## HBase HFile Structure
HFile包含多层索引，这允许HBase在寻找数据的时候不用读取全部的文件。多层的索引就想一个B+Tree：
>* KV键值对按照底层顺序存储。
>* 索引逐行扫描64KB块中的数据。
>* 每一个块拥有自己的叶子索引
>* 每个块的最后一个key放在中间索引中
>* 根索引指向中间索引

追踪节点指向meta块，并将数据持久化保存到文件末尾。这些追踪节点包含一些信息，例如：bloom filter和时间范围信息。
Bloom filters有助于跳过文件，这些文件不包含具体的row key。
如果文件不在读取的时间范围内，则时间范围信息对跳过文件有益

## HFile Index
在加载HFile文件的时候，会将索引读取到内存中(BlackCache)。这种方式允许使用单个磁盘搜索执行查找。

## HBase Read Merge
每个MemStore有多个HFiles，在读取数据，需要检查多个文件，这样会影响性能，这就是读放大。
在刷新数据到磁盘上时，MemSotre会随着时间的推移会创建多个小的存储文件。

## HBase Minor Compaction
HBase会自动收集小的HFile，然后将数据写入到大点的HFile中。这个过程为minor compaction。
minor compaction通过将大量小文件写入到少量的大文件中(将小文件进行排序合并，写入大文件中)。

## HBase Major Compaction
Major Compaction将区域中的所有HFile文件合并以及重写为每一个column family的一个HFile。
在这个过程中删除已经删除的或者过期的单元。这种方式提高性能，但是磁盘读写和网络流量增长比较大。
这就是写扩张。

Major Compaction可以设置自动运行。由于写扩张，Major Compaction一般会被安排在晚上或者周末。
Major Compaction在节点挂掉或者因为负载均衡，会将远程的文件放到其他的节点上。

## Region = Contiguous Keys
Region简介：
>* 一个表可以水平拆分为一个或者多个region。一个region包含开始key和结束key之间的已经排序的连续key
>* 每个region默认大小为1G
>* 表的region服务于RegionServer提供的客户端
>* 一个regionserver可以服务1000个region(这些region有可能是同一个表也有可能是多个表)

## Region Split
在初始化的时候，每个表有一个region。随着region变大，将会把这个region拆分为两个小的region。
两个小region(各有一半原先region的数据)在相同的Region Server中并行打开，然后将拆分信息报告给HMaster。
由于负载均衡的原因，HMaster有可能会将新产生的region调度到其他RegionServer中。


HBase region最大的容量设置为：hbase.hregion.max

## Read Load Balancing
Region拆分最先发生在相同的Region Server，但是为了负载均衡的原因，HMaster有可能将新产生的region调度到其他regionserver中。
这就导致RegionServer才远程的HDFS节点上读取数据直到发生major compaction之后，将数据移动到新的regionserver本地节点上。

HBase在写入数据的时候，都是写入本地的，但是当region发生移动之后，只有在发生major compaction之后，数据才会写入到本地。

## HDFS Data Replication

所有读/写操作都来自于主节点。HDFS备份WAL和HFile块。HFile块的复制操作是自动发生的。
HBase依赖于HDFS将数据存储为文件来保证数据安全。当把数据写入到HDFS时，一份写入本地，第二份写入到secondary node，第三份写入第三个节点。

MemStore是存储在内存中，没有进行备份。WAL进行备份，用来容错。

## HBase Crash Recovery
当RegionServer挂啦，奔溃的Region变成不可用状态，这些都会被检测到，并且开启恢复操作。
Zookeeper通过regionserver的心跳信息来知道节点是否挂啦。如果regionserver挂啦，HMaster节点会接到通知。

当HMaster检测到regionserver奔溃，HMaster会将崩溃regionserver中的region重新分配到活跃的regionserver上。
为了恢复regionserver中那些没有写入磁盘的数据，HMaster会将崩溃regionserver的wal拆分为独立的文件并且在新的regionserver节点
的数据节点上存储这些文件。每个regionserver然后重放被分离出的WAL，然后重建region的MemSotre。
>* 重新分配region
>* 分离出崩溃region的WAL
>* 重放WAL，重建region的MemStore

## Data Recovery

WAL文件包含一系列的编辑操作，一个编辑操作表示一个单独的修改或者删除操作。
编辑是按照时间顺序发生的，对于持久化，将数据追加到存储在磁盘上的WAL文件中。

如果节点失败，数据仍然在内存中，并且没有持久化到HFile中，如何处理？
通过读取WAL文件内容来重放编辑操作，将包含的编辑操添加到当前的MemSotre，并对这些编辑操作进行排序。
最后，MemSotre刷新将修改写入到HFile中。

## Apache HBase Architecture Benefits
HBase提供以下好处：
>* 强一致模型：如果执行写操作，所有的读取操作都会返回相同的值。
>* 自动扩展：a)在数据增长比较大之后，达到最大值之后，会将region拆分；b)使用HDFS来传播和复制数据
>* 内置恢复：使用WAL文件来恢复数据
>* 于Hhadoop结合：HBase的数据都是存储在HDFS上

## 参考文献
文章翻译自[An In-Depth Look at the HBase Architecture](https://mapr.com/blog/in-depth-look-hbase-architecture/)