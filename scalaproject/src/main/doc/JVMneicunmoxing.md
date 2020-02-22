# JVM内存模型
Java 虚拟机的内存空间分为 5 个部分:程序计数器、Java虚拟机栈、本地方法栈、堆、方法区。

JDK 1.8 同 JDK 1.7 比，最大的差别就是：元数据区取代了永久代。元空间的本质和永久代类似，都是对 JVM 规范中方法区的实现。不过元空间与永久代之间最大的区别在于：元数据空间并不在虚拟机中，而是使用本地内存。

|区域|是否线程共享|是否会内存溢出|
|----|----|-----|
|程序计数器|否|不会|
|Java虚拟机栈|否|会|
|本地方法栈|否|会|
|堆|是|会|
|方法区|是|会|

## 程序计数器
### 程序技术器的定义
程序计数器是一块较小的内存空间，是当前线程正在执行的那条字节码指令的地址。若当前线程正在执行的是一个本地方法，那么此时程序计数器为Undefined。
### 程序计数器的作用 
字节码解释器通过改变程序计数器来依次读取指令，从而实现代码的流程控制。
在多线程情况下，程序计数器记录的是当前线程执行的位置，从而当线程切换回来时，就知道上次线程执行到哪了。

### 程序计数器的特点
线程私有，它的生命周期与线程相同。
可以看做是当前线程所执行的字节码的行号指示器。
在虚拟机的概念模型里（仅是概念模型，各种虚拟机可能会通过一些更高效的方式去实现），字节码解释器工作时就是通过改变这个计数器的值来选取下一条需要执行的字节码指令，如：分支、循环、跳转、异常处理、线程恢复（多线程切换）等基础功能。 
如果线程正在执行的是一个Java方法，这个计数器记录的是正在执行的虚拟机字节码指令的地址；如果正在执行的是Natvie方法，这个计数器值则为空（undefined）。
程序计数器中存储的数据所占空间的大小不会随程序的执行而发生改变，所以此区域不会出现OutOfMemoryError的情况。


## Java虚拟机栈
>* 线程私有的，它的生命周期与线程相同。
>* 虚拟机栈描述的是Java方法执行的内存模型：每个方法被执行的时候都会同时创建一个栈帧（Stack Frame）用于存储局部变量表、操作栈、动态链接、方法出口等信息。每一个方法被调用直至执行完成的过程，就对应着一个栈帧在虚拟机栈中从入栈到出栈的过程。
>* 局部变量表存放了编译期可知的各种基本数据类型（boolean、byte、char、short、int、float、long、double）、对象引用（reference类型），它不等同于对象本身，根据不同的虚拟机实现，它可能是一个指向对象起始地址的引用指针，也可能指向一个代表对象的句柄或者其他与此对象相关的位置）和returnAddress类型（指向了一条字节码指令的地址）。局部变量表所需的内存空间在编译期间完成分配，当进入一个方法时，这个方法需要在帧中分配多大的局部变量空间是完全确定的，在方法运行期间不会改变局部变量表的大小。
>* 该区域可能抛出以下异常：当线程请求的栈深度超过最大值，会抛出 StackOverflowError 异常；栈进行动态扩展时如果无法申请到足够内存，会抛出 OutOfMemoryError 异常。

### 压栈出栈过程
当方法运行过程中需要创建局部变量时，就将局部变量的值存入栈帧中的局部变量表中。

Java 虚拟机栈的栈顶的栈帧是当前正在执行的活动栈，也就是当前正在执行的方法，PC 寄存器也会指向这个地址。只有这个活动的栈帧的本地变量可以被操作数栈使用，当在这个栈帧中调用另一个方法，与之对应的栈帧又会被创建，新创建的栈帧压入栈顶，变为当前的活动栈帧。

方法结束后，当前栈帧被移出，栈帧的返回值变成新的活动栈帧中操作数栈的一个操作数。如果没有返回值，那么新的活动栈帧中操作数栈的操作数没有变化。



## 本地方法栈

与虚拟机栈非常相似，其区别不过是虚拟机栈为虚拟机执行Java方法（也就是字节码）服务，而本地方法栈则是为虚拟机使用到的Native 方法服务。虚拟机规范中对本地方法栈中的方法使用的语言、使用方式与数据结构并没有强制规定，因此具体的虚拟机可以自由实现它。甚至有的虚拟机（譬如Sun HotSpot 虚拟机）直接就把本地方法栈和虚拟机栈合二为一。与虚拟机栈一样，本地方法栈区域也会抛出StackOverflowError和OutOfMemoryError异常。


## 堆
被所有线程共享，在虚拟机启动时创建，用来存放对象实例，几乎所有的对象实例都在这里分配内存。对于大多数应用来说，Java堆（Java Heap）是Java虚拟机所管理的内存中最大的一块。
Java堆是垃圾收集器管理的主要区域，因此很多时候也被称做“GC堆”。如果从内存回收的角度看，由于现在收集器基本都是采用的分代收集算法，所以Java堆中还可以细分为：新生代和老年代；新生代又有Eden(新创建的对象)空间、From Survivor空间、To Survivor空间三部分。

Young:Old = 1:2
Eden:Survior= 4:1(因为有两个Survior)
Eden:SurviorFrom:SurivorTo = 8:1:1
Java 堆不需要连续内存，并且可以通过动态增加其内存，增加失败会抛出 OutOfMemoryError 异常。
老年代：对象存活时间比较长（经过多次新生代的垃圾收集，默认是15次）的对象则进入老年的。

## 方法区
用于存放已被加载的类信息、常量、静态变量、即时编译器编译后的代码等数据。
和 Java 堆一样不需要连续的内存，并且可以动态扩展，动态扩展失败一样会抛出 OutOfMemoryError 异常。
对这块区域进行垃圾回收的主要目标是对常量池的回收和对类的卸载，但是一般比较难实现，HotSpot 虚拟机把它当成永久代（Permanent Generation）来进行垃圾回收。
方法区逻辑上属于堆的一部分，但是为了与堆进行区分，通常又叫“非堆”。

## 运行时常量池
运行时常量池是方法区的一部分。
Class 文件中的常量池（编译器生成的各种字面量和符号引用）会在类加载后被放入这个区域。
除了在编译期生成的常量，还允许动态生成，例如 String 类的 intern()。这部分常量也会被放入运行时常量池。

在 JDK1.7之前，HotSpot 使用永久代实现方法区；HotSpot 使用 GC 分代实现方法区带来了很大便利；
从 JDK1.7 开始HotSpot 开始移除永久代。其中符号引用（Symbols）被移动到 Native Heap中，字符串常量和类引用被移动到 Java Heap中。
在 JDK1.8 中，永久代已完全被元空间(Meatspace)所取代。元空间的本质和永久代类似，都是对JVM规范中方法区的实现。不过元空间与永久代之间最大的区别在于：元空间并不在虚拟机中，而是使用本地内存。因此，默认情况下，元空间的大小仅受本地内存限制。


## 直接内存
直接内存（Direct Memory）并不是虚拟机运行时数据区的一部分，也不是Java虚拟机规范中定义的内存区域，但是这部分内存也被频繁地使用，而且也可能导致OutOfMemoryError 异常出现。在 JDK 1.4 中新加入了 NIO 类，引入了一种基于通道（Channel）与缓冲区（Buffer）的 
I/O方式，它可以使用 Native 函数库直接分配堆外内存，然后通过一个存储在 Java 堆里的 DirectByteBuffer 
对象作为这块内存的引用进行操作。这样能在一些场景中显著提高性能，因为避免了在Java 堆和 Native 堆中来回复制数据。


### 直接内存与堆内存比较
直接内存申请空间耗费更高的性能
直接内存读取 IO 的性能要优于普通的堆内存。
直接内存作用链： 本地 IO -> 直接内存 -> 本地 IO
堆内存作用链：本地 IO -> 直接内存 -> 非直接内存 -> 直接内存 -> 本地 IO



## 启动线程的三种方式
Thread、Runnable、Executors.newCachedThread。

## synchronized
synchronized是对某个对象进行加锁。

非synchronized方法可以和synchronized方法同时运行。如果避免脏读,直接添加都添加synchronized。

synchronized如何实现可重入锁？
可以调用父类,或者子类的方法，都是使用的同一个锁。

程序出现异常情况,默认会释放锁。

JDK早期，synchronized实现是重量级的，找操作系统申请锁，效率低。
后来,synchronized做了改进:
访问某把锁的时候,现在object(markword)头上记录线程,没有加锁,如果在判断的时候，是同一个锁,直接执行(自选锁)，如果是其他线程，申请锁，升级为自旋锁，默认为判断10次,如果没有获取到锁，然后升级到重量锁。


AtomicXXX(大部分使用的是自选锁)只占用CPU,不会访问操作系统,在用户态解决锁的问题，不会使用内核态。
重量锁不使用CPU,执行时间长的,线程数多尽量使用系统锁(重量锁synchronized);加锁代码执行时间短、线程数少的情况下尽量使用自旋锁。

synchronized锁定对象的时候不能使用基础类型(String(不能使用String常量)、Integer、Long)


JVM内置锁通过synchronized使用,通过内部对象Monitor(监视器锁)对象,基于进入与退出Monitor对象实现方法与代码同步块,监视器锁的实现依赖底层操作系统的Mutex Lock(互斥锁)的实现，它是一个重量级锁效率低。
monitorenter进入同步代码块，monitorexit离开同步代码块。


JVM内置锁,有没有方法能够手动控制加锁与解锁？
JDK6之前不能操作,JDK6的时候提供unsafe类。unsafe只能被bootstrapclassloader加载。需要通过反射方式来获取unsafe。


实例对象内存中存储在哪儿?
对象存储在堆中,对象的引用存储在线程栈空间,对象的元数据存在方法区或者元空间。

synchronized锁升级优化 JDK6之后，性能提升很高。synchronized在锁升级的过程中，需要转换为偏向锁(默认开启)，锁只能升级，不能降级。



## volatile可见性底层实现原理
JMM内存交互层面:volatile修饰的变量的read、load、use操作和assign、store、write必须是连续的，即修改后立即修改回主内存,使用时必须从主内存刷新,由此保证volatile变量的可见性。

底层实现:通过汇编Lock指令,它会锁定变量缓存行区域并写回主内存,这个操作称之为缓存锁定。
缓存一致性机制会阻止同时修改被两个以上处理器缓存的内存区数据(MSI协议)。
一个处理器的缓存回写到内存，内存会导致其他处理器的缓存无效(MESI协议)。
volatile是Java虚拟机提供的轻量级同步机制,它具备两种特性:
>* 保证共享变量对所有线程的可见性
>* 禁止指令重排序优化，保证有序性
>* 无法保证原子性:要保证原子性需要借助synchronized、Lock锁机制,同时也能保证有序性与可见性,因为synchronized和Lock都能保证同一时刻只有一个线程访问该代码块。





指令重排：cpu为了执行效率。
程序重排:导致程序执行不确定性。

怎么防止指令重排(不用volatile)？
不能使用synchronized，由于jit优化,将这个synchronized去掉


定义volatile的变量,可以通过下面的当时拿到变量的值

```
public class UnsafeInstance{
	public static Unsafe reflectGetUnsafe(){
		try{
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			return (Unsafe)field.get(null);
			}catech(Exception e){
				e.printStackTrace();
			}
			return null;
	}
}
```
```
private volatile int state = 0;
private final static Unsafe unsafe = UnsafeInstance.reflectGetUnsafe();
private final static long stateOffset;
static{
	ry{
		stateOffset = unsafe.objectFieldOffSet(TradeService.class.getDeclaredField("state"));
		}catch(Exception e){
			throw new Error(e);
		}
}
```

## HashMap
使用哈希为了均匀分布
使用数组为了快速查找
使用链表为了解决冲突
使用红黑树为了查找速度


初始容量必须为2的幂次。如果不是2的次幂，在执行操作的时候转换为2的次幂。
为什么要转为次幂:不取模为了效率,为了元素都在索引范围内使用位运算。


加载因子为设置为0.75? 时间和空间平衡:loadfactor 1 空间占满,查询效率慢;loadfactor 0.5 空间占用率低,频繁扩容,查询效率高。

JDK8为什么为8时设置为红黑树:哈希碰撞,泊松分布:在效率最高的时候,设置的值为8。

JDK7扩容死锁与环链分析:
JDK7在扩容的时候,



## 如何识别垃圾
>* 引用计数(不能解决循环引用)
>* 根可达算法:


标记清除算法:产生碎片、位置不连续
拷贝算法:没有碎片、浪费空间;效率高
标记压缩算法:没有碎片、效率偏低


新生代+老年代+永久代(JDK7)/元数据区MetaSpace(JDK8):
>* 永久代/元数据区 存储的都是class文件
>* 永久代必须制定大小;元数据区可以设置，也可以不设置,无上限(受物理内存限制);
>* 字符串常量存放与永久代(JDK7)/堆(JDK8)
>* MethodAread 是一个逻辑概念,对应于JDK7的永久代/JDK8的元数据区。


### JVM 训练营
创建新对象先放入栈,
栈里面放不了，对象很大，放入老年代,


G1 逻辑分代 物理不分代，物理分区，可以回收部分区域

Serial + Serial Old。
Parallel Scavenge + Parallel Old
ParNew + CMS

Stop the World(STW):所有的线程都停止。


垃圾回收器的发展和内存大小相关。

CMS(Concurrent Mark Sweep): 工作线程和GC线程并行。

有多少个根对象:main函数中new的对象个数。

CMS如何解决漏标？
三色标记算法。

CMS重大问题:会产生碎片，当碎片足够多的时候(使用serial old,时间很长),

JDK8使用G1垃圾回收器。


提高吞吐量:PS + PO

响应时间:STW时间短

优化JVM运行环境:如果出现慢、卡顿，提高内存，时间更短,换垃圾回收器。

Jstack waiting on condition id  很多的时候,CPU居高不小,有可能是出现了死锁。


jmap -histo pid | head 

jmap -dump:format=b,file=20200208.dump pid


jad  查看内容
redefine 重新加载某个类文件

内存占10%，总产生FGC: 自己写的System.gc


gc root   finalize finally 


## 类加载器分析
类加载器读取的都是class文件。

class文件的来源:本地磁盘、网络下载class文件、war，jar下加载.class文件、将源文件动态编译成.class文件。

将class文件加载到 JVM的方法区。

启动的方法为Launcher

bootClassPath:sun.boot.class.path 
appclassloader: java.class.path
extclassloader: java.ext.dirs

ClassLoader主要方法:
loadClass():
findClass():
defineClass():

除了根加载器以外,每一个加载器有且仅有一个父加载器。


Thread.currentThread().setContextClassLoader(this.loader); 可以打破双亲委派模型


双亲委派模型的好处:
保证Java核心类下的类型安全。借助双亲委派模型，Java核心类库的类必须是有启动类加载器加载的,可以确保Java核心类库只会在JVM中存在一份，这样就不会给自定义类加载器去加载核心类库。

父类加载器加载的类不能访问子类类加载器加载的类。

打破双亲委派模型之线程上下文加载器:JDBC接口技术之SPI之应用。

类的首次主动使用会触发类的初始化:
>* 调用静态方法
>* 给静态变量赋值获取一个静态变量
>* 反射Class.forName()
>* new出一个对象
>* 执行main方法的时候
>* 初始化子类会初始化它的父类


## 红包
点击红包之后，弹出窗口,然后才能点击打开:这是削峰限流。
关于double的计算，一定要使用BigDecimal。
先抢到红包，存储到Redis,不能及时到账,然后执行安全这一套。保证数据最终一致性。
如果打开成功,后面出现异常,人工对账。

抢红包和记账分离:


拆红包场景分析:
拆红包场景不能出现用户重复拆同一个红包:使用缓存来记录用户是否拆过这个红包。
不能出现用户拆到红包的金额合计超过或者小于红包的总金额:每次取0-- (红包剩余金额)/2的随机数，也要保证没人最少抢到一分钱。
不允许手快的没拆而手慢的拆到红包:为更新请求使用行锁(使用悲观锁,性能差;如果使用带有version的乐观锁,就会出现手快的没有抢到,手慢的抢到了),将所有的更新红包请求、更新红包账户、写拆红包记录请求放入到redis的List中,实现串行化。 
根据红包拆分规则，将红包拆好,然后放入到缓存中,


tcc与事务入侵很大。


分布消息事务


## 秒杀
商品查询-->创建订单(加入购物车-->确认订单-->修改库存-->待支付)-->支付订单-->卖家发货。

秒杀的特点:
>* 短时高并发、负载压力大
>* 读多写少
>* 竞争资源有限,不能多卖，不能少卖，不能重卖



在模拟高并发的情况下，需要批量发送大量请求时,可以使用CountDownLatch来设置数量。

CycleBarrier来记录开始的时候

乐观锁的实现:
>* mysql + 版本号(校验库存是否一致、减少库存、版本加1,要保证原子性)
update t_goods_info 
set amount = amount - {buy},version =version + 1
where code = #{code} and version = #{version}

如果出现失败,可以重试,然后等待一段时间,再次重试,可以实现削峰(错峰执行),大量调用可能出现栈溢出。
>* 通过状态控制:
update t_goods_info 
set amount = amount - #{buys}
where code = #{code} and amount - #{buys} >=  0


使用数据库实现乐观锁简单高效,问题可靠;缺点是并发能力低。

>* 基于redis实现乐观锁
利用watch指令在redis事务中提供cas能力。

wtach key #监控key
multi # 开启事务
set key value # 设置值
exec # 提交事务


>* 基于memcache实现乐观锁
gets 和CAS实现
gets 获取数据
cas 减库存


CAS是否可以彻底解决秒杀的问题？
>* 页面:按钮置灰,禁止用户重复提交请求,通过js来控制在一定时间内只能提交一次请求
>* 应用层:动静分离,压缩缓存处理(CDN);利用uid限频，页面缓存技术;反向代理+负载均衡
>* 服务层:读写操作基于缓存；请求处理排队，分批放行；热点分离
>* 读写分离；分库分表；数据库集群


## nginx
nginx 反向代理
nginx 动静分离
nginx 静态模版
nginx 缓存
nginx 调用服务


## Redis

setnx

主线程出现异常 使用try finally
服务宕机 设置key的时间
锁时间低于执行时间 删除其他请求的锁
防止删除其他线程的锁 使用随机字符串设置为值

Redission Lock:RedLock

缓存雪崩:

缓存穿透:

### String结构
单值缓存
set key value 
get value
对象缓存
set key value(json格式数据)
mset(批量设置) user:1:name zhuge user:1:balance 1888
mget(批量获取) user:1:name user:1:balance
批量设置或者获取 性能差

分布式锁实现:
setnx(存入一个不存在的字符串键值对) product:10001 true  //返回1代表获取锁成功，0表示获取锁失败。
del product:10001 
setnx product:10001 true ex 10 nx //设置过期时间放置因为程序挂，导致死锁

String 应用场景:
计数器:
incr article:readcount:{articleID}
GET article:readcount:{articleID}

Web集群Session共享
spring session+redis实现session共享

分布式系统全局序列号
incrby orderid 1000

### Hash结构(类似于Map)

对象缓存
hmset user 1:name zhuge 1:balance 1888
hmget user 1:name  1:balance

hash 实现购物车

添加商品:hset cart:1001 1088 1
增加数量:hincrby cart:1001 1088 1
商品总数:hlen cart:1001
删除商品:hdel cart:1001 1088
获取购物车所有商品:hget cart:1001

### list(链表)
常用的数据结构
栈:LPush + LPop -->FILO
Queue:LPush+RPop
Blocking MQ:LPush+BRPop

 微博和微信公众号信息流
 关注大V
 LPush msg:18888(ID) 10086
 LPush msg:18888(ID) 10087
 获取最新数据
 LRange msg:18888 0 5

### Set 数据结构

#### 微信抽奖小程序
10086添加抽奖
SADD activity:1000 10086
SADD activity:1000 10087

查看所有的用户
smembers activity:1000

抽奖
srangemember activity:1000 2 

抽多个奖
spop activity:1000 1 //抽中之后不会在set中存在

#### 微信微博点赞、收藏、标签
点赞
SADD like:1000 10086{userId}

取消点赞
SREM like:1000 10086{userId}

检查用户是否点过赞
sismember like:1000 10086{userId}

获取点赞的列表
smember like:1000 

获取点赞的个数
scard like:1000

集合操作(关注列表)
A关注的人 ASET
B关注的人 BSET
C关注的人 CSET
A和B共同关注的人 SINTER ASET BSET //ASET和BSET的交集
A关注的人也关注B
SISMEMBER BSET A //判断A是否在BSET中
SISMEMBER CSET A

A可能认识的人
SDIFF CSET BSET //B和C的差集


#### 附近的人
添加一个或多个地理空间位置到sorted set
geoadd key longitude latitude member [longitude latitude member]
geoadd key 120 30 "beijing"

GEOHASH 返回一个标准的地理空间的Geohash字符串。
GEOHASH key member [member ...]
geohash key beijing

GEOPOS //显示经纬度
返回地理空间的经纬度
geopos key beijing

GEODIST //返回两个地理空间之间的距离
GEODIST key member1 member2 [unit]
geodist key beijing shanghai

GEORADIUS //查询指定半径内所有的地理空间元素的集合。
GEORADIUS key longitude latitude radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]

geohash key 120 30 1000 m


GEORADIUSBYMEMBER //查询指定半径内匹配到的最大距离的一个地理空间元素

GEORADIUSBYMEMBER key member radius m|km|ft|mi [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count]

georadiusbymember key beijing 1 km

## 全局ID
业务编码+时间戳+机器编码+随机4位数+毫秒数

UUID优点:
简单,代码方便
生成id性能更好,基本不会有性能问题
全球唯一,可以很好的应对数据迁移
缺点:
没有排序,无法保证趋势递增
使用字符串存储,查询的效率低
存储空间大
传输数量大


数据库集群的话,如何解决自增id等性能问题？
设置步长，增加集群节点,会出现数据混乱。


guava RateLimiter 来限流


网关 责任链模式:减少查询频率;频率校验;权限查询;其他校验。



## TCC
 

## 牙膏 参半小太阳鱼子酱牙膏120 * 2

在Java的对象初始化过程中，一个实例变量最多可以被初始化4次。

## 多生产者 多消费者问题

if 判断一次会产生虚假唤醒
```
public synchronized void increment() throws InterruptedException {
    if (count != 0){
        this.wait();
    }
    count++;
    this.notifyAll();
}
```
while 可以避免虚假唤醒
```
public synchronized void increment() throws InterruptedException {
    while (count != 0){
        this.wait();
    }
    count++;
    this.notifyAll();
}
```

## CopyOnWriteArrayList
```
List<String> result = new ArrayList<>();
for (int i = 0;i < 10;i++){
       new Thread(()->{
           result.add(UUID.randomUUID().toString().substring(0,5));
           System.out.println(result);
       },String.valueOf(i)).start();
   }
```
上面的代码会出现ConcurrentModificationException异常。
解决方法:
```
List<String> a = new Vector<>();
List<String> b = Collections.synchronizedList(new ArrayList<>());
List<String> c = new CopyOnWriteArrayList<>();
```
copyonwrite 写时复制，在写入的时候避免覆盖，避免造成数据丢失。
读写分离:写入的时候复制一份，写完更换指针

CopyOnWriteArrayList vs Vector:
CopyOnWriteArrayList 使用的Lock来保证线程安全 Vector使用的是synchronized来保证线程安全。


## CopyOnWriteArraySet
```
Set<String> a = Collections.synchronizedSet(new HashSet<>());
Set<String> b = new CopyOnWriteArraySet<>();
```
CopyOnWriteArraySet可以保证在多线程下set的安全
HashSet的底层结构为HashMap,

## Map
Map是不安全的

## Callable
可以抛出异常、可以有返回值、执行call方法

```
class CustomCallable implements Callable<Integer>{
    @Override
    public Integer call() throws Exception {
        return 23423;
    }
}
//调用方法
CustomCallable customCallable = new CustomCallable();
FutureTask<Integer> futureTask = new FutureTask<>(customCallable);
new Thread(futureTask).start();
System.out.println(futureTask.get());
```
get方法会阻塞，结果有缓存。

## BlockingQueue
|方法类型|抛出异常|返回值|阻塞|超时|
|----|----|----|----|----|
|插入|add()|offer()|put(e)|offer(time,unit)|
|移除|remove()|poll()|take()|poll(time,unit)|
|检查|element()|peek()|不可用|不可用|


Java 中的阻塞队列
>* ArrayBlockingQueue :由数组结构组成的有界阻塞队列。
>* LinkedBlockingQueue :由链表结构组成的有界阻塞队列。
>* PriorityBlockingQueue :支持优先级排序的无界阻塞队列。
>* DelayQueue:使用优先级队列实现的无界阻塞队列。
>* SynchronousQueue:不存储元素的阻塞队列。
>* LinkedTransferQueue:由链表结构组成的无界阻塞队列。
>* LinkedBlockingDeque:由链表结构组成的双向阻塞队列


## 线程池
CPU密集型:Runtime.getRuntime().availableProcessors();//获取CPU核数,可以保持CPU效率最高
IO密集型:判断你程序中耗IO的操作,大于其线程数就可以。

## 函数式接口
函数式接口:只有一个方法的接口

Function: 有一个输入,有一个输出:对元素做一些处理。apply方法

Predicate: 一个输入参数,返回值只能是布尔值,用来判断是否符合要求。test方法

Consumer: 只有输入,没有输出,做一些不需要有返回值的判断。accept方法

Supplier: 没有输入,只有返回值。get方法。

Stream流:

forkJoin: 工作窃取:性能好处理完任务,然后去其他线程获取任务执行。 

```
//使用fork/join来处理
public static void test() throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    ForkJoinPool pool = new ForkJoinPool();
    ForkJoinTask<Long> task = new ForkJoinDemo(1L,100000000L);
    ForkJoinTask<Long> submit = pool.submit(task);
    Long result = submit.get();
    System.out.println(result);
    System.out.println(System.currentTimeMillis() - start);
}
//使用流来处理
public static void test1() throws ExecutionException, InterruptedException {
    long start = System.currentTimeMillis();
    long result = LongStream.rangeClosed(0,100000000L).parallel().reduce(0,Long::sum);
    System.out.println(result);
    System.out.println(System.currentTimeMillis() - start);
}
```


## 异步回调

CompleteFuture:
```
public static void test2() throws ExecutionException, InterruptedException {
    //无返回值
    CompletableFuture<Void> completableFuture = CompletableFuture.runAsync(()->{
        try {
            TimeUnit.SECONDS.sleep(2);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(Thread.currentThread().getName());
    });
    System.out.println("===1===");
    completableFuture.get();
}

public static void test3() throws ExecutionException, InterruptedException {
    //有返回值
    CompletableFuture<Integer> completableFuture = CompletableFuture.supplyAsync(()->{
        System.out.println("===31===");
        return 1;
    });
    System.out.println("===32===");
    System.out.println(completableFuture.get());
    System.out.println(completableFuture.whenComplete((t,u)->{
        System.out.println("t:"+t); //返回正确的值
        System.out.println("u:"+u); //错误的信息
    }).exceptionally(e->{
        System.out.println(e.getMessage());
        return 1024; //返回错误结果
    }).get());
}
```



## 参考文献
[JVM内存模型（jvm 入门篇）](https://www.jianshu.com/p/a60d6ef0771b)
[JVM内存模型](https://juejin.im/post/5ad5c0216fb9a028e014fb63)
[JVM 内存模型](https://segmentfault.com/a/1190000015398964)
[JVM 完整深入解析](https://segmentfault.com/a/1190000014395186)