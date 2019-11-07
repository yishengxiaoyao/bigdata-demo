内容基于Hive:0.9.0版本

Hive不支持记录级别的更新、插入或者删除操作。

Hive不支持OLTP(联机事务处理)。

Hive最适合数据仓库应用程序。

Hive中使用SQL来写wordcount:
```
create table docs(line string);
load data inpath 'docs' overwrite into table docs;
create table word_counts as 
select word,count(1) as count from 
 (select explode(split(line,'\t')) as word from docs) w 
 group by word 
 order by word;
```

hivevar(define于hivevar一样功能) 用户自定义变量;<br> hiveconf Hive相关配置属性;<br>system Java定义的配置属性;<br>env shell环境定义的环境变量。
```
hive --define foo=bar;
set hivevar:foo=bar;
set hiveconf:hive.cli.print.current.db=true;
set system:user.name=xiaoyao;
set evn:TERM=xterm;
```
set hive.cli.print.current.db=true; --展示当前的数据库名称

set hive.exec.mode.local.auto=true; --运行本地模式

set hive.cli.print.header=true; --显示字段名称

查找命令:
hive -S -e "set" |grep warehouse  #S表示静默模式。

$HOME/.hivehistory 执行的命令

执行shell命令: !/bin/echo  "what's up!";

执行dfs命令: dfs -ls / ;

注释需要以 --开头。

hive集合数据:struct('John','Doe')、map('first','JOIN','last','Doe'')[结果为first->JOIN,last->Doe]、Array('John','Doe')

```
create table  employees(
 name STRING,
 salary FLOAT,
 suborinates ARRAY<STRING>,
 deductions MAP<STRING,STRING>,
 address STRUCT<street:STRING,city:STRING,state:STRING,zip:INT>
 )
 ROW FORMAT DELIMITED 
 FIELDS TERMINATED BY '\001' 
 COLLECTION ITEMS TERMINATED BY '\002'  
 MAP KEYS TERMINATED BY '\003'
 LINES TERMINATED BY '\n' 
 PARTITIONED BY (country STRING,state STRING)
 STORED AS TEXTFILE; 
```
set hive.metastore.warehouse.dir; --指定数据仓库的路径，default数据库就在这个目录下，其他的数据库，会在这个目录下，创建相应的文件夹。默认的路径为/user/hive/warehouse。

hive不允许用户删除一个包含有表的数据库。

TBLPROPERTIES的主要作用是按键值对为表增加额外的文档说明。

FORMATTED(最常用)输出的信息比EXTENDED的信息还要多。

set hive.mapred.mode=strict; --对分区表进行查询而where子句没有加分区过滤，将禁止提交这个任务。

```
CREATE EXTERNAL TABLE IF NO EXISTS log_messages(
   hms INT comment '时分秒',
   severity STRING comment '验证成都',
   server   STRING comment '服务器', 
   process_id INT comment '进程号',
   message  STRING comment '消息'
)
PARTITIONED BY (year INT,month INT,day INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
```

fs.trash.interval 设置删除文件在回收站存放的时间，存放在/user/$USER/.Trash目录。

创建钩子：ALTER TABLE log_messages TOUCH PARTITION(year=2012,month=1,day=1)

打包文件:ALTER TABLE log_message ARCHIVE PARTITION(year=2012,month=1,day=1)，这个功能只适用于独立分区。


ALTER TABLE log_message ARCHIVE PARTITION(year=2012,month=1,day=1) ENABLE NO_DROP;--防止分区被删除<br>
ALTER TABLE log_message ARCHIVE PARTITION(year=2012,month=1,day=1) ENABLE OFFLINE;--防止分区被查询

|属性名称|缺省值|描述|
|----|----|----|
|hive.exec.dynamic.partition|false|设置为true，开启动态分区功能|
|hive.exec.dynamic.partition.mode|strict|设置为nostrict，表示允许所有分区都是动态的|
|hive.exec.dynamic.partition.prenode|10|每个mapper或reducer可以创建的最大分区个数|
|hive.exec.max.dynamic.partitio|1000|一个动态分区可以创建的最大动态分区个数|
|hive.exec.max.created.file|10000|全局可以创建的最大文件个数|

explode:将行数据转换为列数据;<br> concat_ws:将列数据拼成一个行数据。

```
select name,salary,
    case 
        when salary<5000.0 THEN 'low'
        when salary>=5000.0  AND salary<7000.0 THEN 'middle'
        when salary>=7000.0 AND salary<10000.0 THEN 'high'
        ELSE 'very high'
    END AS bracket from employees;
```
需要将hive.exec.mode.local.auto=true;设置在$HOME/.hiverc,这样可以避免进行mapreduce任务。

不能在where语句中使用列别名(如果使用列别名，结果集中没有包含相应的列)。

Hive只支持等值连接，目前还不支持ON子句中的谓词间使用OR。

大多数情况下，Hive会对每对JOIN连接对象启动一个MapReduce任务(从右往左的顺序执行)。

Hive在执行语句时，会先执行JOIN语句，然后在执行Where过滤操作。

Hive可以在map端执行连接过程(map-side join)，这是因为Hive可以和内存中的小表进行逐一匹配，从而忽略常规操作所需要的reduce过程。<br>
这样不仅减少了reduce过程，而且有时还可以减少map过程的执行步骤。

在执行是map-side join时，需要添加/*+MAPJOIN(d) */的标记(0.7版本之前需要加)。<br>
如果不加这个标记，可以设置hive.auto.convert.join=true;在必要的时候启动这个优化,<br>
也可以设置优化小表的大小,hive.mapjoin.smalltable.filesize=25000000(单位为字节).

如果涉及到分桶连接的化，需要设置下面的参数:
```
set hive.input.format=org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
set hive.optimize.bucketmapjoin=true;
set hive.optimize.bucketmapjoin.sortedmerge=true;
```

SORT BY 只会在每个reducer中对数据进行排序(局部排序)，ORDER BY是全局的。

DISTRUCTE BY控制map的输出在reducer中如何划分的。DISTRUCTE BY要放在SORT BY之前。

CLUSTER BY= DISTRUCTE BY+SORT BY，这样会减少SORT BY的并行度，输出的数据为全局排序的。

cast(salary as FLOAT):将字段salary转换为FLOAT类型。

视图都是只读的，只允许修改TBLPROPERTIES属性信息。

可以将下面的语句优化:
```
原始语句:
INSERT OVERWRITE TABLE sales select * from history  where action='purchased';
INSERT OVERWRITE TABLE credits select * from history  where action='returned';
优化后的语句:
from history 
insert overwrite sales select * where action='purchased' 
 insert overwrite credits select * where action='returned';
```
在进行分桶存储数据时，需要设置参数，保证分桶数和reducer数量一致。
```
set hive.enforce.bucketing=true;
或者
set mapred.reduce.tasks=90; --设置reduce具体的数量;
```

一个Hive任务会包含一个或多个stage，不同的stage之间会存在依赖关系。

Hive开启并行操作:hive.exec.parallel=true;

Hive提供了一个严格模式,为了防止用户执行那些可能产生意想不到的不好的影响的查询。<br>
通过设置hive.mapred.mode值为strict可以禁止3种类型的查询:
* 对于分区表，除非where语句中含有分区字段过滤条件来限制数据范围，否则不允许执行(不允许扫描所有分区)。
* 对于ORDER BY语句的查询,要求必须使用LIMIT语句，为了防止reducer额外执行很长一段时间。
* 限制笛卡尔积的查询，在执行JOIN查询的时候，不使用ON语句而是使用WHERE语句，关系型数据库的执行器可以高效的将WHERE转换为ON语句，Hive不会执行这种优化。

Hive是按照输入的数量的大小来决定reducer的个数的。reducer个数=(集群中reducer槽位的个数*1.5)/执行中的查询的平均个数

JVM重用是Hadoop调优参数的内容，特别是对于很难避免小文件的场景或task特别多的场景。<br>
JVM重用可以是的JVM实例在同一个Job中重新使用N次。可以在mapred-site.xml中设置mapred.job.reuse.jvm.num.tasks=10。

推测执行(mapred-site.xml)开启:
```
mapred.map.tasks.speculative.execution=true;
mapred.reduce.tasks.speculative.execution=true;
Hive提供了配置型来控制reduce-side的推测执行:
hive.mapred.reduce.tasks.speculative.execution=true;
```
Hadoop的job通常是IO密集型而不是CPU密集型的。

Hive中设置压缩格式: set io.compression.codec;

使用压缩的优势是可以最小化所需要的磁盘存储空间，以及减少磁盘和网络IO操作。但是，文件压缩和解压会增加CPU开销。

Hive开启中间压缩:hive.exec.compress.intermediate=true(默认为false);

Hadoop job控制中间数据压缩的属性为:mapred.compression.map.output，在mapred-site.xml或者hive-site.xml中设置。

SnappyCodec是一个比较好的中间文件压缩编码器/解码器，很好结合低CPU开销和好的压缩执行效率。

最终的输出压缩:hive.exec.compress.output=true,mapred.outpu.compression.codec=org.apache.hadoop.io.compression.GzipCodec.

SequenceFile有三种压缩方式:NONE、RECORD、BLOCK(压缩性能最好而且可以分割的)。如果需要的化，需要设置mapred.output.compression.type=BLOCK(压缩格式必须为SequenceFile).

explode()函数是以array类型作为输入，然后对数组中的数据进行迭代，返回多行结果，一行一个数组元素值。
```
select name,sub from employees LATERAL VIEW explode(subordinates) subView as sub;
```
LATERAL VIEW方便的将explode这个UDAF得到的行转列的结果集合在一起提供服务。<br>
使用LATERAL VIEW需要指定视图别名(subView)和生成新列的别名(sub)。


如果想要自己的UDF方法可以在Hive中成为默认的方法，可以修改FunctionRegistry.java中的静态方法块，添加进去尽可以了。


```
每个分类的前3个:
select type,country,product,sales_count,rank 
from (
	select type,country,product,sales_count,
	rank() over(PARTITION BY type,country ORDER BY sales_count desc) rank 
	from p_rank_demo) t where rank<=3;
```


是否可以分片取决于Compression。

ORC parquet.
