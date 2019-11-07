#ORC File
ORC 文件是在hive 0.14.0开始支持。

##ORC 文件格式
相对于其他的文件格式，ORC文件格式有以下优点:
>* 每个任务的输出是一个单独的文件，这样可以减少NameNode的负载
>* Hive类型支持包括:datetime,decimal和复杂类型
>* 在文件中存储轻量级的索引
>   + 跳过不传递谓词过滤的行组
>   + 寻找特定的行
>* 基于数据类型的块模式压缩
>   + 整数列的形成编码
>   + 字符串列的字典编码
>* 使用不同的RecordReader可以并发读取同一个文件
>* 可以在不扫描标记的情况进行分割文件
>* 限制读或写所需的内存量
>* 使用Protocol Buffer来存储源数据，允许添加和删除字段
## File Structure
ORC文件是由stripe、file footer、postscript。
stripe:index data、group of row data、stripe footer;默认大小为250M；大的stripe可以实现HDFS的高校读。
file footer：辅助信息,文件中包含的所有stripe信息、每个stripe含有的数据行数、每一行的数据类型、列级别的聚合操作(count、min、max、sum)。
postscript:包含压缩参数和压缩页脚大小。

## Strip Structure
每一个Strip包含:index data、row data、striper footer。
striper footer包含流位置的目录。row data用于表扫描。
Index data包含每一列的最大值和最小值以及每一行的位置。行索引提供了offset，这样可以在解压块找到相应的压缩快和字节。
ORC索引只用于查找strip和行数据，不用于回答查询。

默认情况下，可以跳过10000行。


## 参数设置
|参数|默认值|解释|
|----|----|----|
|hive.exec.orc.memory.pool|5|在写文件时，heap的最大值|
|hive.exec.orc.default.stripe.size|256M(0.13.0),64M(0.14.0)|默认的stripe大小|
|hive.exec.orc.default.block.size|256M|默认数据块大小|
|hive.exec.orc.default.row.index.stride|10000|strip默认的行数|
|hive.exec.orc.default.buffer.size|256K|默认的缓存区大小|
|hive.exec.orc.default.compress|ZLIB|默认的压缩方式|
|hive.exec.orc.encoding.strategy|SPEED|写数据默认的编码策略，有两种SPEED和Compression|
|hive.orc.cache.stripe.details.size|10000|每个stripe可以缓存的大小|
|hive.orc.compute.splits.num.threads|10|并行度|
|hive.exec.orc.split.strategy|HYBRID|拆分策略:BI(不从HDFS中读取数据，进行与拆分)、ETL(在拆分前，读取文件footer)、HYBRID(如果文件小于预期的mapper数量，读取所有文件的footer，在平均文件大小小于HDFS默认大小的情况下，每个文件生成一个分区)|
|hive.exec.orc.zerocopy|true|是否使用零复制来读取数据|
|hive.exec.orc.compression.strategy|SPEED|写数据的压缩格式，有SPEED和COMPRESSIONS|
|hive.orc.row.index.stride.dictionary.check|true||
