# JAVA SPI 机制
## SPI是什么

SPI全称Service Provider Interface，是Java提供的一套用来被第三方实现或者扩展的API，它可以用来启用框架扩展和替换组件。
Java SPI 实际上是“基于接口的编程＋策略模式＋配置文件”组合实现的动态加载机制。
系统设计的各个抽象，往往有很多不同的实现方案，在面向的对象的设计里，一般推荐模块之间基于接口编程，模块之间不对实现类进行硬编码。
为了实现在模块装配的时候能不在程序里动态指明，这就需要一种服务发现机制。


## 适用场景

>* 数据库驱动加载接口实现类的加载
>* JDBC加载不同类型数据库的驱动

>* 日志门面接口实现类加载
>* SLF4J加载不同提供商的日志实现类

Spring
>* Spring中大量使用了SPI,比如：对servlet3.0规范对ServletContainerInitializer的实现、自动类型转换Type Conversion SPI(Converter SPI、Formatter SPI)等

Dubbo
>* Dubbo中也大量使用SPI的方式实现框架的扩展, 不过它对Java提供的原生SPI做了封装，允许用户扩展实现Filter接口


## 使用介绍
要使用Java SPI，需要遵循如下约定：

1、当服务提供者提供了接口的一种具体实现后，在jar包的META-INF/services目录下创建一个以“接口全限定名”为命名的文件，内容为实现类的全限定名；

2、接口实现类所在的jar包放在主程序的classpath中；

3、主程序通过java.util.ServiceLoder动态装载实现模块，它通过扫描META-INF/services目录下的配置文件找到实现类的全限定名，把类加载到JVM；

4、SPI的实现类必须携带一个不带参数的构造方法；


## 原理解释
首先看ServiceLoader类的签名类的成员变量：
```
public final class ServiceLoader<S>
    implements Iterable<S>
{

private static final String PREFIX = "META-INF/services/";

//被加载的类或者接口
private final Class<S> service;

//用于定位,加载和实例化providers的类加载器
private final ClassLoader loader;

//创建serviceloader时采用的访问控制上下文
private final AccessControlContext acc;

// 缓存provider,按实例化的顺序排序
private LinkedHashMap<String,S> providers = new LinkedHashMap<>();

// 懒查找迭代器
private LazyIterator lookupIterator;
```

### 应用程序调用ServiceLoader.load方法



ServiceLoader.load方法内先创建一个新的ServiceLoader，并实例化该类中的成员变量，包括：

loader(ClassLoader类型，类加载器)

acc(AccessControlContext类型，访问控制器)

providers(LinkedHashMap类型，用于缓存加载成功的类),

lookupIterator(实现迭代器功能)

### 应用程序通过迭代器接口获取对象实例


ServiceLoader先判断成员变量providers对象中(LinkedHashMap类型)是否有缓存实例对象，如果有缓存，直接返回。
如果没有缓存，执行类的装载：

读取META-INF/services/下的配置文件，获得所有能被实例化的类的名称

通过反射方法Class.forName()加载类对象，并用instance()方法将类实例化

把实例化后的类缓存到providers对象中(LinkedHashMap类型）
然后返回实例对象。



## 总结
### 优点
使用Java SPI机制的优势是实现解耦，使得第三方服务模块的装配控制的逻辑与调用者的业务代码分离，而不是耦合在一起。应用程序可以根据实际业务情况启用框架扩展或替换框架组件。

### 缺点

虽然ServiceLoader也算是使用的延迟加载，但是基本只能通过遍历全部获取，也就是接口的实现类全部加载并实例化一遍。如果你并不想用某些实现类，它也被加载并实例化了，这就造成了浪费。获取某个实现类的方式不够灵活，只能通过Iterator形式获取，不能根据某个参数来获取对应的实现类。
多个并发多线程使用ServiceLoader类的实例是不安全的。
## 参考文献
[高级开发必须理解的Java中SPI机制](https://yq.aliyun.com/articles/640161)
