# String 优化
## String 对象是如何创建的
JDK6以及之前的版本中，String对象是对char数组进行了封装实现的对象,主要的成员变量为:char数组、offset偏移量、字符数量count、哈希值hash。
String对象可以通过offset和count可以定位char[]数组,获取字符串。这样可以高效、快速地共享数组对象，同时节省内存空间，这种方式有可能会导致内存泄漏。

JDK7/JDK8，String类中不再有offset和count两个变量,这样做的好处是String对象占用的内存稍微少了些,String.substring()方法也不再共享char[],
从而解决使用该方法可能导致的内存泄漏问题。

JDK9:添加了新的属性,coder,提供了一个编码格式的标识。便于查找数据的位置。

String默认大小为16。

## String的不可变性
char[] 被private final 修饰，代表了String对象不可被更改,Java实现的这个特性叫做String对象的不可变性,即String对象一旦创建成功，
就不能在对它进行改变。不可变性的作用在于当一个对象需要被多线程共享,并且频繁访问时，可以忽略同步和锁等待的时间,从而大幅度提高系统性能。
保证hash属性值不会频繁变更:确保了唯一性，使得类似HashMap容器才能是想相应的Key-value缓存功能。
可以实现字符串常量池。两种创建方式:
>* String str="abc";JVM会检查该对象是否在常量池中，如果窜爱，返回应用，否则在常量池中创建新的字符串。这样可以减少同一个值的字符串对象的重复创建，节约内存。
>* String str = new String("abc");在编译类文件时，"abc"常量字符串将会放入常量结构中,在类加载时，"abc"将会在常量池中创建;其次,在调用new时,
JVM命令将会调用String的构造函数，同时引用常量池中的"abc"字符串,在对内存中创建一个String对象,最后,str引用String对象。

Java中比较两个对象是否相等,往往是用==,要判断两个对象的值是否相等,则需要用equals方法来判断。

针对常量池的优化:当两个String对象拥有相同的值时，只饮用常量池中的同一个拷贝。

类final的定义:作为final类的String对象在系统中不能有任何子类,这是对系统安全性的保护。

## String对象的优化
### 如何构建超大字符串
在单线程或者保证线程安全的情况下使用StringBuilder最优。
在并发情况下不保证线程安全则使用StringBuffer。
不要使用字符串累加。

### 如何使用String.intern节省内存
在每次赋值时使用String的intern方法，如果常量池中有相同值,就会重复使用该对象,返回对象应用。

调用intern方法，会去查看字符串常量池中是否有等于该对象的字符串对象,如果没有,在JDK6中会复制堆中的字符到常量池中，并返回该字符串引用。
堆内存中原油的字符串由于没有应用指向它,将会通过垃圾回收机制回收。在JDK7之后,常量池合并到堆中,只会把首次遇到的字符串的引用添加到常量池
中，如果有，就返回常量池中的字符串应用。
常量池的实现类似于HashTable的实现方式,HashTable存储的数据越大,遍历的时间复杂度就会增加。如果数据量过大，会增加整个字符串常量池的负担。

### 如何使用字符串的分割方法 
String.indexOf()(也可以使用String.charAt())和String.substring()结合替代String.split()
如果碰到制定分割分隔符的字符串，可以使用StringTokenizer来进行拆分。

### String的长度限制
在编译时，String的长度不能超过65535，在运行时不能超过Integer.MAX_VALUE。

## String vs StringBuilder
String 与StringBuilder的另一个区别在于当实例化String时,String可以利用构造函数、赋值的方式来初始化;StringBuilder只能通过构造函数来初始化。

## 参考文献
[Java字符串性能优化](https://blog.csdn.net/weixin_37948888/article/details/97815980)
[Java 性能优化之 String 篇](https://www.ibm.com/developerworks/cn/java/j-lo-optmizestring/)
[Java性能优化之String字符串优化](https://www.cnblogs.com/xuqiang7/p/10917903.html)
[Java | 字符串性能优化不容小觑，百M内存轻松存储几十G数据](https://blog.csdn.net/sinat_27143551/article/details/103033564)
[JAVA语言核心精讲4--String/StringBuffer/StringBuilder区别](https://blog.csdn.net/sinat_27143551/article/details/80774548)