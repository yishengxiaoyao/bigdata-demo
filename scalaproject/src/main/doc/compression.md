# Compression
## 压缩的优点
1.减少网络传输，节省带宽
2.节省存储空间
##压缩的缺点
1.解压文件增加CPU开销

压缩场景: input temp output

|compression format|tool|algorithm|file extension|Splitable|
|-----|------|-----|-----|----|
|gzip|gzip|deflate|.gz|no|
|bzip2|bzip2|bzip2|.bz|yes|
|lzo|lzop|lzop|.lzo|yes if indexed|
|snappy|N/A|Snappy|.snappy|no|


压缩比:bzip2>gzip>lzo

压缩率:lzo>gzip>bzip2


|use compressed map input|compress intermediate data|compress reducer output|
|-----|-----|-----|
|mapreduce jobs read input from hdfs<br>compres if input data is large.This will reduce disk read cost<br>compress with splittable algorithm<br>use compress with splittable file structures|map output is written to disk||



hive支持的文件格式:Text File,SequenceFile,RCFile(0.6.0),Avro Files,ORC Files,Parquet。

hiveserver2默认的端口为10000。

hiveserver2 支持多用户、安全。

hive不适用于线上事务处理，适用于传统数据仓库任务。

hive3支持事务和update/delete/merge。


|参数|含义|默认值|
|-----|-----|-----|
|hive.exec.mode.local.auto|是否开启本地模式|false|
|mapreduce.framework.name(0.7版本以及之后)|运行的模式||
|hive.exec.mode.local.auto.inputbytes.max|本地模式，输入文件的大小|128M|
|hive.exec.mode.local.auto.tasks.max|本地模式map任务的最大值|4|
|hive.mapred.local.mem|mapreduce任务的最大内存|0|
|hive.querylog.location|执行sql的存储路径|从1.1之后，需要设置hive.log.explain.output|
本地模式reduce任务可以是1或者0。

replace columns只修改table的schema，不修改数据，必须使用native SerDe。

如果使用的版本比较就，在查询数量的时候，需要使用count(1)替换count(*)。

ALTER DATABASE...SET LOCATION这个语句，只修改新添加表的数据库位置，已经存在的，不修改。

hive的表和列是大小写不敏感，但是SerDe和属性是大小写敏感的。

hive的注释都是单引号。

可以通过hive.default.fileformat设置文件的存储格式，默认为文本格式。

set -v:列出Hadoop和Hive的配置变量。

!<command>:从hive shell中执行shell命令。

dfs <command>:从hive shell中执行dfs命令。

beeline替换hive cli，可以是内置的和远程连接到hiveserver2。

hive的输出格式:数据表(默认)、vertical(行列转换)等。

beeline连接到hiveserver2的默认端口为10000，如果是transportMode=http，默认的端口为10001，还可以使用zookeeper。

hive变量只在当前session中生效，在相同的session，多次设置相同的变量，使用最后一个值。变量替换默认为开启状态，可以通过set hive.variable.substitute=false;来关闭。

hcat 主要是对表进行权限控制。


