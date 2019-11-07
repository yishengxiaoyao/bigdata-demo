# Scala Doc

This is used to explain how to use the scala.

## Prerequisites

1. [jdk.18](https://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html)
2. [scala-2.11.8](https://www.scala-lang.org/download/2.11.8.html) 

## Installation

1.jdk
```bash
tar -zxvf jdk-8u191-linux-x64.tar.gz -C /user/local/java
vi /etc/profile
export JAVA_HOME=/user/local/java/jdk1.8.0_191
export PATH=$JAVA_HOME/bin:$PATH
```
2.Scala
```bash
tar -zxvf scala-2.11.8.tgz -C /user/local/
vi /etc/profile
export $SCALA_HOME=/user/local/scala-2.11.8
export PATH=$SCALA_HOME/bin:$PATH
```

```bash
source /etc/profile
java -version
    java version "1.8.0_191"
scala -version
    Scala code runner version 2.11.8 -- Copyright 2002-2016, LAMP/EPFL
```
## Demo

1. 创建maven项目，按照scala模版创建，等到IDEA自己将需要的jar加载进来
2. 修改pom文件，将原先创建的scala文件删除

```bash
<properties>
    <scala.version>2.11.8</scala.version>
    <hadoop.version>2.6.0-cdh5.7.0</hadoop.version>
    <spark.version>2.2.0</spark.version>
  </properties>
```
3.创建object：创建HelloWorldApp object.
```bash
package com.edu.spark

object HelloWorldApp {
  def main(args: Array[String]): Unit = {
    println("Hello World!!!")
  }
}
```
4.运行
在这个类中选择右键，选择Run HelloWorldApp选项，查看输出为：
```bash
Hello World!!!
```
## Scala CLI

1. 系统默认变量
 
```
scala> 3+5
res0: Int = 8
```
在上面的代码中，res是result的缩写，后面的0表示变量的标记，是从开始，后面：Int表示这个变量为int类型，=8表示值为8。

2.自定义变量

2.1 常量
```bash
scala>  val money:Int = 10000
money: Int = 10000
scala> money:Int = 18000
<console>:1: error: ';' expected but '=' found.
money:Int = 18000
```
使用val 定义的变量为常量，一旦赋值之后不能进行修改，如果需要重新赋值，可以使用下面的方法：
```bash
scala>  val money:Int = 10000
money: Int = 10000
scala> val money:Int = 18000
money: Int = 18000
```

2.2 变量
```bash
scala> var money:Int = 10000
money: Int = 10000
scala> money = 18000
money: Int = 18000
```
定义一个变量可以直接进行修改。

可以将val认为是value的缩写，是一个指定的值，不能变，而var是variable的缩写，是一个可变的量。

3. 类型推导
```bash
scala> var city="beijing"
city: String = beijing
```
scala可以直接进行类型推导，就是根据值来推导变量的类型，也可以自己指定类型。

4. 数据类型

```bash
scala> var money:Float=1000.54;
<console>:11: error: type mismatch;
 found   : Double(1000.54)
 required: Float
       var money:Float=1000.54;
                       ^
scala> var money:Float=1000.54f;
money: Float = 1000.54
```
在对Float、Long数据类型的数据进行操作时，需要指定类型，例如1000.54f,100000L,20000.234d。
```bash
scala> 10.isInstanceOf[Int]
res2: Boolean = true
```
判断某个变量或者值是否为某个特定的类型。

```bash
scala> 10.asInstanceOf[Long]
res3: Long = 10
```
类型转换：将这个值进行类型转换，变成其他数据类型，要考虑之间的兼容。


## 函数定义

函数定义结构如下：
```bash
def  函数名(参数以及类型):返回值类型={  //方法体
  //不需要return，最后一行的数据当作函数的返回值
}
例句：
def add(x: Int, y: Int): Int = {
    x + y
}
其他方式如下：
def add1(x: Int, y: Int) = {
    x + y
}
def add2(x: Int, y: Int)= x + y

如果这个方法没有返回值，可以使用下面的写法：
def sayHello(): Unit ={
    print("say hello")
}
在调用无参数的函数时，可以直接使用函数名来调用，不需要携带后面的括号
```

## 函数应用

1. to

```bash
scala> 1 to 10
res5: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
scala> 1.to(10)
res6: scala.collection.immutable.Range.Inclusive = Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
上面两种操作，结果是一样的，都是一个range对象,左闭右闭。
```

2.until

```bash
scala> 1 until 10
res7: scala.collection.immutable.Range = Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
scala> 1.until(10)
res8: scala.collection.immutable.Range = Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
上面两种操作，结果是一样的，都是一个range对象，左闭右开。
```

3.Range
```bash
Range定义：
class Range(val start: Int, val end: Int, val step: Int)
设置开始start，结束值end，每次的步长 step(默认为1)
scala> Range(1,10)
res10: scala.collection.immutable.Range = Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
将Range对象变成可变参数：(1 to 10 :_*)
```

4. 对比

Range、until、to产生的对象都是Range
Range 与 until 基本一致，都是左闭右开的集合 从结果可以看出，比to 缺少一个inclusive，表示不包含最右边这个数
to 是左闭右闭的集合 从结果可以看出，比Range，until多一个inclusive，表示包含最右边这个数


## 遍历

1. 遍历列表

```bash
foreach 对前面的集合中的每一个元素都要执行一次这样的操作
Array("Hadoop","Spark","Hive","Flink").foreach(x=>println(x))
或者
for(x<-Array("Hadoop","Spark","Hive","Flink")){
      println(x)
}
含有判断条件的遍历：
for(i<- 1 to 10 if(i%2 == 0)){
      println(i)
}
```
## 默认参数

在方法中，有的参数有一个默认值，这个参数为默认参数，在调用拥有默认参数的方法时，需要带上括号，可以不传参数

```bash
def sayName(name:String="xiaoyao"): Unit ={
    println(name)
}
```

## 命名参数

就是指定参数的名称。

```bash
def speed(distance:Float,time:Float)={
    distance/time
}
调用时，下面两种方式都可以
println(speed(100,5))
println(speed(distance = 100,time = 5))
```

### 可变参数

可以有任意个数的参数。

```bash
def sum(argv:Int*) ={
    var result = 0
    for (element<- argv){
      result+=element
    }
    result
}
调用方式为：
println(sum(3,4))
println(sum(3,4,5))
println(sum(1 to 10 : _*))  
```
在scala中使用class作为关键字，定义相应的类。在定义属性时，可以使用占位符来定义变量,必须要指定类型：
```
val name:String=_
```
属性私有化(只能内部使用)：
```
private[this] var gender="male"
```
子类中如果添加新的字段，需要添加修饰符。

## Scala注意事项
1. 在Scala中声明private变量,Scala编译器会自动生成get,set方法 
2. 在Scala中变量需要初始化
3. 在Scala中没有静态修饰符,在object下的成员全部都是静态的,如果在类中声明了与该类相同的名字的object则该object是该类的”伴生对象”
可以理解为Scala把类中的static集中放到了object对象中,伴生对象和类文件必须是同一个源文件,可以用伴生对象做一些初始化操作.
4. 在Java中可以通过interface实现多继承,在Scala中可以通过特征(trait)实现多重继承,但是与Java不同的是,它可以定义自己的属性和实现方法体
5. object不能提供构造器参数,也就是说object必须是无参的

## Object和Class的区别
1. 在Scala中,类名可以和对象名为同一个名字,该对象称为该类的伴生对象,类和伴生对象可以相互访问他们的私有属性,但是它们必须在同一个源文件中
2. 类只会被编译,不能直接执行,类的声明和主构造器在一起被声明,在一个类中,主构造器只有一个.
3. 类和它的伴生对象可以相互访问其私有成员
4. class和object的一个差别是,object里面的属性或者方法都是单例的，单例对象不带参数,而类可以.因为你不能用new关键字实例化一个单例对象,你没有机会传递给它参数
5. object在调用时，按照从上到下依次执行，不调用的方法不执行。object中的方法或者属性可以通过object.method/object.properties, 不需要通过new方式获取对象。
6. # class要获取对象必须使用new方式，然后才能调用方法或者属性,object不需要。
7. 互为伴生对象+() 调用对象的apply方法,并且创建class实例。

```
(例如：var b=ApplyTest() // oject ApplyTest.apply())
```
8.如果为class实例+(),调用类自己的apply方法。

```
例如：
var c=new ApplyTest
println(c)
c()  //class apply
```

参考自：https://blog.csdn.net/qq_33813365/article/details/77869661

## 属性

1. 在类中需要定义一些属性，一般使用var来进行定义，在定义时，有时候没有默认值，需要使用占位符来表示，并且要表明属性的类型，否则会报错
2. 私有属性：只能在类内部访问，不能通过创建对象的方式来访问私有属性。

```
定义属性
var name:String=_
// var name="doudou"
定义私有属性
//private[this]不允许其他对象访问，对象私有，只能这个对象访问，这个对象的类中的方法也不可以访问这个成员。其他对象中的方法也不可访问。
private[this] var gender="male"
```

## 构造器
1. 在创建对象时，使用关键字new时，就是调用构造函数的时候。
2. 使用class来修饰的就是主构造器，可以是有参也可以是无参。在主构造器中可以创建附属构造器，也就是除了主构造器之外的其他构造器
3. 附属构造器中的第一行必须是主构造器或者其他附属构造器。每次实例化对象时，都需要使用到主构造器。

## 继承
关键字为extends
1.在继承时，可以继承主构造器也可以继承附属构造器。
2.在继承时，在主构造器方法中拥有的属性不是父类的属性，需要使用var来修饰这个属性。
3.如果需要对属性或者方法进行重写，需要使用关键字override，来进行重写属性或者函数。在重写属性时，需要将属性使用val来进行定义。


## 抽象类
1.抽象类中一个或者多个方法只有定义没有实现(方法体)。
2.抽象类不能直接使用，必须要有具体的实现类。

## 集合

### Array
1.概念
数组是定长的一个集合，在创建的时候已经确定整个数组的长度，不能进行增加或者减少，只能在原先的数据上进行操作。
在定义数组时，如果没有进行赋值时，默认为数组类型的默认值,数组的索引是从0开始的，一直到数组长度减去1，即[0,Array.length-1)。
如果下标不再这个范围时，会输出java.lang.ArrayIndexOutOfBoundsException异常。
元素可以重复,元素类型都一样

1.1定义固定长度的数组
```bash
var array=new Array[String](5) //数组元素的类型为string，默认值为null，数组的长度为5。
```
1.2定义拥有初始化值的数组
```bash
var array=Array(0,0,0,0,0) //等同于上面的定义操作，在创建时调用apply方法，会自动创建相应的长度的array，并将值复制到相应位置。 
```
2.操作

2.1数组长度
```bash
scala> var array=new Array[Int](5)
scala> array.length
res0: Int = 5
```
2.2更新元素
```bash
使用下标更新数据
array(2)=2
array(2,5)
```
2.3最值
```bash
array.max
array.min
```
2.4求和
```bash
array.sum
```
2.5拼接
```bash
scala> array.mkString(",")
res11: String = 0,5,2,3,4
也可以指定开始和结束的符号，中间使用什么进行连接
scala> array.mkString("[",",","]")
res12: String = [0,5,2,3,4]
```
2.6最后一个元素
```bash
array.last
```

2.7变成vector
```bash
println(array.to[Vector])
//可以写成下面的方式
println(array.toVector)
```

2.8是否包含
```bash
array.contains(2) //包含 true，不包含 false
```

2.8反转
```bash
array.reverse.mkString(",) 
```

2.9 合并两个数组
```bash
Array.concat(array,b.toArray).mkString(",")
```

2.10 判断是否为空
```bash
array.isEmpty
array.nonEmpty
```

2.11拷贝数据到其他数组中
```bash
Array.copy(array,1,b.toArray,1,2)
```
2.12 生成一个数组
```bash
Array.range(1,5)
Array.range(1,5,2) //步长默认为1
```

2.13生成一个空的数组
```bash
Array.empty  //也可以指定参数类型
```


### ArrayBuffer

1.概念
ArrayBuffer是一个变长的数据，就是长度不固定，可以动态的增加或者删除元素。
元素可以重复。

2.操作

2.1添加元素

```bash
//增加单个元素
scala> var b=ArrayBuffer[Int]()
b: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer()
scala> b+=1
res13: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1)
//增加多个元素,使用++来进行操作
scala> b++=Array(6,7,8)
res15: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 6, 7, 8)
scala> b++=ArrayBuffer(6,7,8)
res16: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 6, 7, 8, 6, 7, 8)
//使用insert 可以指定插入的位置，可以同时插入多个元素，在指定的下标之后插入
scala> b.insert(1,6,7)
scala> b
res26: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 6, 7, 2, 3, 4, 5)
//append
scala> b.append(6)
scala> b
res45: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(4, 1, 2, 3, 4, 5, 6)
```
2.2删除元素
```bash
//删除单个元素
scala> b-=1
res13: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(6,7,8)
scala> b--=Array(6,7,8)
res15: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(6, 7, 8)
scala> b--=ArrayBuffer(6,7,8)
res16: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer()
//使用remove执行操作，可以移除指定下标的值，或者从当前下标开始往后的几个元素
scala> b.remove(1)
res28: Int = 6
scala> b
res29: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 7, 2, 3, 4, 5)
scala> b.remove(1,3)
scala> b
res31: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 4, 5)
//使用trimEnd(从最右边开始数几个数移除)或者trimStart(从最左边开始数几个数移除),他们的底层实现为remove
scala> b.trimEnd(1)
scala> b
res33: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 4)
scala> b.trimStart(1)
scala> b
res35: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(4)
```
2.3拼接

与Array相同，请参考上面的操作。

2.4获取数据

```bash
//从左边开始，取几个数据
scala> b.take(2)
res39: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(4, 1)
//取后边几个数据
scala> b.takeRight(3)
res41: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(3, 4, 5)
```

2.5转换为array

```bash
scala> b.toArray
res43: Array[Int] = Array(4, 1, 2, 3, 4, 5)
```

2.6 tail
```bash
//获取除了第一个元素之外的所有元素
scala> b.tail
res42: scala.collection.mutable.ArrayBuffer[Int] = ArrayBuffer(1, 2, 3, 4, 5)
```
2.7遍历
```bash
//遍历
for(element<-b){
      println(b)
}
```
2.8获取数据的索引
```bash
b.indexOf(2) //从左侧开始数
b.lastIndexOf(3) //上次出现的位置
```
2.9长度比较
```bash
b.lengthCompare(4) // result 1 b的长度比后面的数大
b.lengthCompare(5) // result 0 b的长度与后面的数相等
b.lengthCompare(6) // result -1 b的长度比后面的数小
```

2.10最后一个值
```bash
b.last //数组的最后一个元素
```
2.11变成vector
```bash
println(b.to[Vector])
或者使用下面的方式
println(b.toVector)
```
2.12是否包含某个元素
```bash
b.contains(2) //包含 true，不包含 false
```
2.13反转
```bash
b.reverse.mkString(",") 
```
2.14其他操作
最大值、最小值、求和、以及拼接都与Array操作一样，不再赘述。

## 列表

### List

1. 概念

List 

    a.不可变链式的数据结构存储有序数据，
    b.遵循后进先出原则(栈)，
    c.访问头节点时间复杂度为1，其他元素为0(n)，
    d.结构共享，不需要或者需要固定长度的内容
2.操作

2.1不使用List构建一个List对象
```bash
Nil  
//定义list对象
var l=List(1,2,3,4,5)
var l1=1 :: Nil  //List(1)
var l2=2 :: l1  //List(2,1) //将前面的元素放在最前面，把后面的元素放入到后面
```
2.2第一个元素
```bash
l.head //1
```
2.3后面的元素
```bash
l.tail //除了第一个之外后面的所有元素,List(2,3,4,5)
```
### ListBuffer
1.操作
1.1添加元素
```bash
scala> lb+=(6,7,8)
res8: scala.collection.mutable.ListBuffer[Int] = ListBuffer(1, 2, 3, 4, 5, 6, 7, 8)
//如果是增加不可变集合，需要使用两个加号
scala> lb++=List(9,10)
res10: scala.collection.mutable.ListBuffer[Int] = ListBuffer(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
```
1.2转成不可变List
```bash
scala> lb.toList
res11: List[Int] = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
```
1.3转成不可变Array
```bash
scala> lb.toArray
res12: Array[Int] = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
```
1.4求和
```bash
def sum(nums:Int*):Int ={
    if(nums.length==0) {
      0
    }else{
      nums.head + sum(nums.tail : _*) //后面这一步是将list变成可变参数
    }
}
```
### Map
1.定义
```
var a=Map("name"->"xiaoyao","age"->30) //不可以修改值
var b=scala.collection.mutable.Map("name"->"xiaoyao","age"->30)
```
1.2获取值
```
println(b.getOrElse("gender","unknown")) //如果没有key，可以输出后面的值，使用b.get("gender")来如果没有值，获取None对象，如果有值获取Some对象，或者使用b("name")直接获取值
```
1.3遍历数据
```
//遍历
for((key,value)<-b){
  println(key+":"+value)
}
for((key,_)<-b){
  println(key+":"+b.getOrElse(key,0))
}
for(key<-b.keySet){
  println(key+":"+b.getOrElse(key,0))
}
for(value<-b.values){
  println(value)
}
```
## 算子操作
### map
map操作是对每一个元素进行相同的操作
```
var l=List(1,2,3,4,5,6,7,8)
for(element<-l){
  println(element)
}
//对集合中的每一个元素都执行相同的操作
println(l.map((x:Int)=>x*2))
println(l.map((x)=>x*2))
println(l.map(x=>x*2))
println(l.map(_ * 2))
```
### foreach
```
//对每一个元素进行遍历
l.map(_*2).foreach(x=>println(x))
l.foreach(x=>println(x))
```
### filter
```
//对元素进行过滤
println(l.map(_*2).filter(_>8))
```
### take
```
//获取多少个元素，必须要指定获取前n个元素,后面这个值必须要填写
println(l.take(3))
```
### sum
```
println(l.sum)
```
### reduce
```
//将相邻的两个数据进行操作
//(1,2,3,4,5,6,7,8)=>(3,3,4,5,6,7,8)=>(6,4,5,6,7,8)=>(10,5,6,7,8)=>(15,6,7,8)=>(21,7,8)=>(28,8)=>(36)
l.reduce((x,y)=>{
  println(x,y)
  x+y
})
println(l.reduceLeft(_+_))
//(1,2,3,4,5,6,7,8)=>(3,3,4,5,6,7,8)=>(6,4,5,6,7,8)=>(10,5,6,7,8)=>(15,6,7,8)=>(21,7,8)=>(28,8)=>(36)
l.reduceLeft((x,y)=>{
  println(x,y)
  x+y
})
println(l.reduceRight(_+_))
//(1,2,3,4,5,6,7,8)=>(1,2,3,4,5,6,15)=>(1,2,3,4,5,21)=>(1,2,3,4,26)=>(1,2,3,30)=>(1,2,33)=>(1,35)=>(36)
l.reduceRight((x,y)=>{
  println(x,y)
  x+y
})
println(l.reduce(_-_))
//(1,2,3,4,5,6,7,8)=>(-1,3,4,5,6,7,8)=>(-4,4,5,6,7,8)=>(-8,5,6,7,8)=>(-13,6,7,8)=>(-19,7,8)=>(-26,8)=>(-34)
l.reduce((x,y)=>{
  println(x,y)
  x-y
})
println(l.reduceLeft(_-_)) //(1,2,3,4,5,6,7,8)=>(-1,3,4,5,6,7,8)=>(-4,4,5,6,7,8)=>(-8,5,6,7,8)=>(-13,6,7,8)=>(-19,7,8)=>(-26,8)=>(-34)
//详细步骤
l.reduceLeft((x,y)=>{
  x-y
})
println(l.reduceRight(_-_)) //(1,2,3,4,5,6,7,8)=>(1,2,3,4,5,6,-1)=>(1,2,3,4,5,7)=>(1,2,3,4,-2)=>(1,2,3,6)=>(1,2,-3)=>(1,5)=>(-4)
//详细步骤
l.reduceRight((x,y)=>{
  println(x+":"+y)
  x-y
})
```
### curry(颗粒化)
```
def sum(a:Int,b:Int)=a+b
//颗粒化
def add(a:Int)(b:Int)=a+b
sum(a:Int,b:Int)=add(a:Int)(b:Int)
```
### fold
```
println("---fold---")
println(l.fold(1)(_+_))   //两个括号连着，就是表示两个参数求和，第一个括号的值可以理解为初始值，加上后面的参数，如果是foldleft，是将这个数放到最左边，如果是foldright，将这个数据放到最右边
//运算规则(1,1,2,3,4,5,6,7,8)=>(2,2,3,4,5,6,7,8)=>(4,3,4,5,6,7,8)=>(7,4,5,6,7,8)=>(11,5,6,7,8)=>(16,6,7,8)=>(22,7,8)=>(29,8)=>(37)
println("---foldleft+---")
l.foldLeft(1)((x,y)=>{
  println(x,y)
  x+y
})
println("---foldRight+---")
//运算规则(1,2,3,4,5,6,7,8,1)=>(1,2,3,4,5,6,7,9)=>(1,2,3,4,5,6,16)=>(1,2,3,4,5,22)=>(1,2,3,4,27)=>(1,2,3,31)=>(1,2,34)=>(1,36)=>(37)
l.foldRight(1)((x,y)=>{
  println(x,y)
  x+y
})
println("---foldminus---")
println(l.fold(1)(_-_))   //(1,1,2,3,4,5,6,7,8)=>(0,2,3,4,5,6,7,8)=>(-2,3,4,5,6,7,8)=>(-5,4,5,6,7,8)=>(-9,5,6,7,8)=>(-14,6,7,8)=>(-20,7,8)=>(-27,8)=>(-35)
l.fold(1)((x,y)=>{
  println(x,y)
  x-y
})
println("---foldrightminus---")
//(1,2,3,4,5,6,7,8,1)=>(1,2,3,4,5,6,7,7)=>(1,2,3,4,5,6,0)=>(1,2,3,4,5,6)=>(1,2,3,4,-1)=>(1,2,3,5)=>(1,2,-2)=>(1,4)=>(-3)
println(l.foldRight(1)(_-_))
l.foldRight(1)((x,y)=>{
  println(x,y)
  x-y
})
println("---foldleftminus---")
println(l.foldLeft(1)(_-_))
//(1,1,2,3,4,5,6,7,8)=>(0,2,3,4,5,6,7,8)=>(-2,3,4,5,6,7,8)=>(-5,4,5,6,7,8)=>(-9,5,6,7,8)=>(-14,6,7,8)=>(-20,7,8)=>(-27,8)=>(-35)
l.foldLeft(1)((x,y)=>{
  println(x,y)
  x-y
})
//reduce操作，默认从左边开始进行加或者减，即前两个数进行相加或者相减，依次往后推进，直到只有一个数据。
//reduceLeft 与reduce操作一致，不再赘述。
//reduceRight 从右边开始往左方向走，先取追右边的两个数，进行相加或者相减，依次往左执行，直到只有一个数据
//fold()()是一个颗粒化操作，相当于将两个参数进行相加(减)；加(减)操作是把第一个参数放在最前面，从左往后加(减)，依次进行相加(减)，直到只有一个数；
//fold指定的方向就是将数据放入到列表的方向:例如 left就是将数据放入到最左边，right就是将数据放入到列表的最右边。
//foldleft()()与fold操作一致
//foldright()()是要将第一个参数放到后面;从右往左进行操作，直到只有一个数。
```
### sum,max,min,count
```
println(l.min+":"+l.max+":"+l.sum)
//可以对数据进行过滤
println(l.count(_>5))
```
### flatmap
```
var f=List(List(1,2),List(3,4),List(5,6))
//将多个集合中的元素放入到一个集合中
println(f.flatten)
//结果为:List(1, 2, 3, 4, 5, 6)
//flatmap=map+flatten
//flatmap是元素放到一个集合中，然后在进行操作
println(f.flatMap(_.map(_*2)))
//map是针对元素的操作
println(f.map(_.map(_*2)))
```
## 文件操作
```
//读取文件
val file=Source.fromFile("/Users/Downloads/test/people.txt")
for(line<-file.getLines()){
  println(line)
}
//指定url
Source.fromURL("")
```
## 隐士转换
动态增强类的功能。
```
//1.定义一个类
class RichFile(val file:File){
  def read()=Source.fromFile(file.getPath).mkString
}
//2.编写隐士转换的方法
implicit  def file2rich(file:File):RichFile=new RichFile(file)
//3.调用
val file=new File("/Users/Downloads/test/people.txt")
val content=file.read()
println(content)
```
## 模式匹配
```
val peoples=Array("xiaoyao","yishengxiaoyao","xiaoyaoyisheng","xiaoyaoshan")
val people=peoples(Random.nextInt(peoples.length))
people match {
  case "xiaoyao" => println("xiaoyao")
  case "yishengxiaoyao" => println("yishengxiaoyao")
  case "xiaoyaoyisheng" => println("xiaoyaoyisheng")
  case "xiaoyaoshan" => println("xiaoyaoshan")
    //没有匹配到的选项
  case _ => println("no match")
}
greeting(Array("zhangsan")) //Hello zhangsan!
greeting(Array("li","wangwu")) //Hello li,wangwu!
greeting(Array("zhangsan","li","wangwu")) //Hello zhangsan and others!
greeting(Array("li","wangwu","zhangsan")) //Welcome!
```
```
def greeting(array: Array[String]): Unit ={
array match {
  case Array("zhangsan") => println("Hello zhangsan!")
  case Array(x,y) => println(s"Hello $x,$y!")
  case Array("zhangsan",_*) => println("Hello zhangsan and others!")
  case _ => println("Welcome!")
}
}
```
## 异常处理
```
try{
  val i=1/0
}catch {
  case e:ArithmeticException => println("除数不能为0!")
  case e:Exception => println("捕获到异常!")
}finally {
  println("the finally module")
}
```
## 偏函数
```
def greetMan(name:String)= name match {
case "zhangsan" => println("Hello zhangsan!")
case "xiaoyao" => println("Hello xiaoyao!")
case _ => println("Welcome!")
}

/**
* 花括号内没有match的一组case就是一个偏函数
* 第一参数为输入值的类型，第二个是输出值的类型
*/
def greetManPartial:PartialFunction[String,String]={
case "zhangsan" => "Hello zhangsan!"
case "xiaoyao" => "Hello xiaoyao!"
case _ => "Welcome!"
}
```

scala case class的属性不能修改。