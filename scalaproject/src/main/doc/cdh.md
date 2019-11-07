#青云部署CDH

##注意
centos7.0 不支持 CDH5.12。

数据库的数据集为UTF-8。

数据库:
https://www.cloudera.com/documentation/enterprise/release-notes/topics/rn_consolidated_pcm.html#concept_xdm_rgj_j1b

机器：2Core  8G内存  Centos7.2

##搭建集群
1.创建VPC网络

创建步骤:网络与CDN-->VPC网络-->填写网络名称(xiaoyaovpcnetwork)-->选择IP地址范围(192.168.0.0/16)-->类型为:小型-->防火墙:选择自己创建的防火墙-->点击确认。

在创建成功之后，点击VPC网络名称进去，选择图形化，就可以VPC网络的图形界面。

2.创建公网IP

申请公网IP:网络与CDN-->公网IP-->申请-->继续申请公网IP-->公网IP名称(hadoopip)-->数量:1-->计费模式:按流量计费-->带宽上线:可以根据自己需求设置-->IP组、绑定方式、ICP备案，选择默认-->选择提交。

3.创建私有网络

创建步骤:网络与CDN-->私有网络-->私有网络名称(xiaoyaoprivatenetwork)-->数量:1-->工作模式:虚拟机(如果自己使用的物理机，可以选择物理机)-->点击提交。

4.创建机器

创建步骤:计算-->主机-->创建-->选择系统7.2 64位-->下一步-->可用区:北京3区-C-->主机类型:基础性-->CPU:4核-->内存:8G-->磁盘:40G->下一步-->选择自己创建的私有网络-->下一步-->计费方式:按需计费-->主机名称:Hadoop-->主机数量:3-->SSH登陆方式:密码(这个可以根据自己爱好选择)-->用户名:root-->密码:自定义的密码-->点击创建按钮(然后修改服务器的名称)。

5.创建防火墙

创建步骤:安全-->防火墙-->名称(xiaoyaofw)-->点击提交按钮。

6.组合VPC网络

6.1添加私有网络

添加步骤:网络与CDN-->选择自己创建的VPC网络-->图形化-->添加添加私有网络

6.2添加公网ip

添加步骤:网络与CDN-->选择自己创建的VPC网络-->图形化-->添加公网IP

6.3端口转发


6.4修改防火墙规则


##准备环境
1.修改hosts文件

由于配置了端口转发，在登陆的时候，需要指定端口
```
localhost:Documents xiaoyao$ ssh -p 222 root@139.198.21.64
```
配置Hosts文件(如果使用了DNS解析,参考其他文章来修改这个文件)
```
[root@hadoop001 ~]# vi /etc/hosts
#添加如下内容，不要删除前两行内容
192.168.110.2 hadoop001
192.168.110.3 hadoop002
192.168.110.4 hadoop003
```
将Host文件，传递到其他机器上:
```
[root@hadoop001 ~]# scp /etc/hosts root@192.168.110.3:/etc/
[root@hadoop001 ~]# scp /etc/hosts root@192.168.110.4:/etc/
```
由于使用的云主机，不需要对防火墙进行操作，如果使用物理机，可以参考相应的文档。

2.安装Jdk(Oracle JDK)

2.1 安装包准备

将JDK相应的安装文件上传到云主机,需要使用端口(因为配置了端口转发):
```
scp -P 222 jdk-8u144-linux-x64.tar.gz root@139.198.21.64:/root
```
然后将文件发送到其他两台服务器上:
```
[root@hadoop001 ~]# scp jdk-8u144-linux-x64.tar.gz root@192.168.110.4:/root
[root@hadoop001 ~]# scp jdk-8u144-linux-x64.tar.gz root@192.168.110.3:/root
```
2.2安装
```
#如果指定的文件夹不存在，然后创建
[root@hadoop001 ~]# tar -zxvf jdk-8u144-linux-x64.tar.gz -C /usr/java
#对文件进行修改权限
[root@hadoop001 ~]# cd /usr/java/
[root@hadoop001 java]# chown -R root:root jdk1.8.0_144/ 
#修改配置文件
export JAVA_HOME=/usr/java/jdk1.8.0_144
export PATH=.:$JAVA_HOME/bin:$PATH
#生效配置文件
[root@hadoop001 java]# source /etc/profile
#验证
[root@hadoop001 java]# java -version
java version "1.8.0_144"
Java(TM) SE Runtime Environment (build 1.8.0_144-b01)
Java HotSpot(TM) 64-Bit Server VM (build 25.144-b01, mixed mode)
```
然后在其他两台机器上执行相同的操作。





















/etc/hosts 里面的第一行和第二行前往不要删除。

如果没有root用户权限,创建Hadoop用户,必须使用sudo无密码。

CDH4.X系列，默认为Python2.6 升级Python2.7.5 HDFS HA不可用。

CDH5.X系列，默认为Python2.6 或者Python2.7

Centos6.x系列 默认Python2.6

Centos7.x系列 默认Python2.7

配置ntp来保证集群的时钟同步


swap 内存磁盘空间 作为内存

swap=0-100

0表示禁用 而且是惰性最高

100表示 积极使用

集群计算实时 要求高 swap=0 允许job挂 迅速的加内存或调大参数 重新启动job

集群计算对实时性 要求不高的  swap=10/30  不允许job挂 慢慢的运行

swap不要超过16G

4G 内存   8G swap


先要安装daemons，然后在安装server、agent。

绿色: 状态好
黄色: 
红色:
灰色:在开始执行的时候，才会有这个进程，在执行完成之后，就没有这个进程啦。

修改配置之后保存的地方:

a)数据库中存储:

b)etc/hadoop/conf 

进程配置文件路径:是会发生变化的
c):/var/run/cloudera-scm-agent/process/62-yarn-resourcemanager。

修改配置的时候，必须要在页面修改。

客户端配置:路径不会发生变化 /etc/hadoop/conf

部署gateway，只有一个/etc/hadoop/conf。

CM架构:
>* 数据存储在MySQL:需要创建cmf、amon
>* 本地仓库 Cloudera Repository:存放离线包
>* Admin Console:7180 username:admin,password:admin
>* instance、role、进程代表同一个
>* Server:cloudera-acm-server(使用的java) agent:cloudera-scm-agent(使用的python)

修改处理过程:web页面修改-->数据库configs表-->/etc/hadoop/*-->/var/run/cloudera-scm-agent/process/*


在使用cloudera api的时候，使用的CDH5.12，使用的api就是：
```
curl -X GET -u "admin:admin" -i \
  http://hadoop001:7180/api/v17/tools/echo?message=hello
```

放置规则:
>* 如果指定的资源池存在,就使用指定的资源池，否则进行下一个放置规则。
常用的放置规则为:root.primary group或者运行时指定。

自己提交job到自己所属的组的资源池 和其他资源池不相干。

HDFS --queue的好处:如果不是HDFS用户,操作HDFS目录，就会有权限限制，所以使用HDFS用户来提交，避免权限问题。



Kafka:

创建文件/var/www/html/kafka_parcels

在添加kafka的时候，如果出现错误，需要增加内存。

Spark2:

Spark 相应的jar放到/opt/cloudera/csd/

chmod 644 csd

chown -R cloudera-scm:cloudera-scm csd

