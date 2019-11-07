分片是有压缩来决定的。

rc 行列混合

text、sequence 行式

orc parquest 列式存储


在编写scala object对象时，可以继承App接口，这样可以不用写main函数。
App接口是将Object变成一个可执行程序。

Scala中的class类似于其他语言中的class。

trait与Java中的接口一样。

Object就是一个单例对象。

如果object继承接口App之后，就不需要写main函数，在App中已经封装了。

在使用Some或者None返回对象时，可以使用Option来接收数据，如果返回的对象是Some，获取数据可以使用get方法;<br>
如果返回的对象是None，就需要使用getOrElse(推荐)来获取值，并且可以设置默认值，也可以使用map的方式来获取。
```
 val glazedDonutTaste: Option[String] = Some("Very Tasty")
    val glazedDonutName: Option[String] = None
    glazedDonutTaste.map(taste => println(s"glazedDonutTaste = $taste"))
    glazedDonutName.map(name => println(s"glazedDonutName = $name"))
```

在运行累加器时，只有在碰到action时，才会触发这个累加器进行处理。
如果多次使用action操作，有可能会导致累加器结果出错。
