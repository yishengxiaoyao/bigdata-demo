#Maxwell的安装和使用

##安装准备工作
Maxwell=MySQL+Kafka.
1.安装MySQL
请参照之前的博客。
在安装完MySQL之后，需要修改my.cnf文件
```
vi /etc/my.cnf
[mysqld]
server-id  = 1
binlog_format = ROW
```
重启MySQL,然后登陆到MySQL之后，查看是否已经修改过来:
```
mysql> show variables like 'binlog_format';
+---------------+-------+
| Variable_name | Value |
+---------------+-------+
| binlog_format | ROW   |
+---------------+-------+
```
在MySQL中添加Maxwell用户,以及分配权限:
```
mysql> create database maxwell;
mysql> CREATE USER 'maxwell'@'%' IDENTIFIED BY '123456';
mysql> GRANT ALL ON maxwell.* TO 'maxwell'@'%';
mysql> GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';
mysql> flush privileges;
```

2.安装Kafka

2.1下载安装包
```
https://archive.apache.org/dist/kafka/0.10.2.1/kafka_2.11-0.10.2.1.tgz
```
2.2解压Kafka安装包
```
[hadoop@hadoop001 app]$ tar -zxvf kafka_2.11-0.10.2.1.tgz 
```
2.3启动Zookeeper
```
[hadoop@hadoop001 app]$ cd zookeeper-3.4.6/bin
[hadoop@hadoop001 bin]$ ./zkServer.sh status
JMX enabled by default
Using config: /home/hadoop/app/zookeeper-3.4.6/bin/../conf/zoo.cfg
Mode: standalone
```
2.4启动Kafka
```
[hadoop@hadoop001 app]$ cd kafka_2.11-0.10.2.1/
[hadoop@hadoop001 kafka_2.11-0.10.2.1]$ bin/kafka-server-start.sh config/server.properties 
```


3.安装Maxwell

3.1下载安装包

下载地址:
https://github.com/zendesk/maxwell/releases/download/v1.20.0/maxwell-1.20.0.tar.gz

3.2解压安装
```
[hadoop@hadoop001 app]$ tar -zxvf maxwell-1.20.0.tar.gz 
```
4.使用
4.1 STDOUT配置

4.1.1在MySQL中创建数据表:
```
create table xiaoyao(id int not null primary key,name varchar(20),age int,address varchar(20));
```
4.1.2开启Maxwell:
```
[hadoop@hadoop001 maxwell-1.20.0]$ bin/maxwell --user=maxwell --password=123456 --host='127.0.0.1' --producer=stdout
```
4.1.3对数据操作:
```
mysql> insert into xiaoyao values(1,'xiaoyao',0,'beijing');
mysql> insert into xiaoyao values(2,'xiaoyao1',20,'beijing');
mysql> update xiaoyao set age=15 where id=1;
```
4.1.4可以在Maxwell中可以看到控制台输出:
```
插入操作:
{"database":"test","table":"xiaoyao","type":"insert","ts":1553397965,"xid":494,"commit":true,"data":{"id":2,"name":"xiaoyao1","age":20,"address":"beijing"}}
更新操作(binlog会记录所有的字段,以及原先的值):
{"database":"test","table":"xiaoyao","type":"update","ts":1553398124,"xid":550,"commit":true,"data":{"id":1,"name":"xiaoyao","age":15,"address":"beijing"},"old":{"age":0}}
```
4.2Maxwell与Kafka结合
```
开启Maxwell:
bin/maxwell --user='maxwell' --password='123456' --host='127.0.0.1' \
   --producer=kafka --kafka.bootstrap.servers=localhost:9092 --kafka_topic=maxwell --kafka_version=0.10.2.1
开启Kakfa:
bin/maxwell --user='maxwell' --password='123456' --host='127.0.0.1' \
   --producer=kafka --kafka.bootstrap.servers=localhost:9092 --kafka_topic=maxwell --kafka_version=0.10.2.1
进入MySQL，修改数据:
mysql> use test;
mysql> update xiaoyao set age=30 where id=2;
开启Kafka消费：
[hadoop@hadoop001 kafka_2.11-0.10.2.1]$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic maxwell --from-beginning
{"database":"test","table":"xiaoyao","type":"update","ts":1553401735,"xid":1675,"commit":true,"data":{"id":2,"name":"xiaoyao1","age":30,"address":"beijing"},"old":{"age":20}}
```

5.Filters

对产生的数据进行过滤:
```
bin/maxwell --user='maxwell' --password='mysqlmaxwellpwd' --host='localhost' \

--producer=kafka --kafka.bootstrap.servers=localhost:9092 \

--kafka_topic=maxwells  --filter 'exclude: ambari.*, include: test_binlog.*'
```
也可以自定义过滤规则，请参考:

http://maxwells-daemon.io/filtering/

http://maxwells-daemon.io/config/


6.关于如何查看binlog

请参考文档：
https://www.cnblogs.com/martinzhang/p/3454358.html

https://blog.csdn.net/a1010256340/article/details/80306952


##Maxwell vs Canal
||Canal(服务端)|Maxwell(客户端+服务端)|
|----|----|----|
|语言|Java|Java|
|活跃度|活跃|活跃|
|HA|支持|定制 但是支持断点还原功能|
|数据落地|定制|落地到Kafka|
|分区|支持|支持|
|bootstrap|不支持|支持|
|数据格式|格式自由|json(格式固定)|
|文档|较详细|较详细|
|随机读|支持|支持|


选择Maxwell的原因:

a.服务端+客户端一体，轻量级的
b.支持断点还原功能+bootstrap+json