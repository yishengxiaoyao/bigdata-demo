#Spark ETL项目
本次项目主要是收集用户的CDN记录，通过记录来获取用户的流量，计算出各种报表。
##架构
web service=>Flume收集日志=>HDFS存储=>ETL=>ORC存储=>ES用来展示数据。

##Flume
* source:taildir
* channel:memory
* sink:HDFS

FLUMe将数据收集过来之后，将数据放入到HDFS文件中，需要对数据进行一个校验。

判断一下生成文文件是否符合标准:文件的个数是否够、文件内容是否有数据。

在判断的过程中，需要在Hive的MetaStore中创建新的假表数据(需要修改hive的源代码,partitions表)。

在判断完成之后，对数据进行处理,然后将数据写入到HDFS上。

