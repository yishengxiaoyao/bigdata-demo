# HashTable vs HashMap vs ConcurrentHashMap

## HashMap
### JDK7 中的HashMap底层实现
#### 基础知识
HashMap继承AbstractMap,实现Map。不管JDK7,还是JDK8,HashMap的实现框架都是哈希表+链表的组合方式。
```
## 这个是JDK8 HashMap的代码
static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16 初始大小,必须为2的次幂
static final int MAXIMUM_CAPACITY = 1 << 30; //哈希表的最大长度
static final float DEFAULT_LOAD_FACTOR = 0.75f; //默认加载因子,如果没有指定,默认值
static final int TREEIFY_THRESHOLD = 8; //树形化的阈值,JDK8
static final int UNTREEIFY_THRESHOLD = 6; //不树形化的阈值 JDK8
static final int MIN_TREEIFY_CAPACITY = 64; //最小树形化的值 JDK8
transient Node<K,V>[] table; //哈希表,存储Entry对象的数组,在JDK7中数组的元素为Entry
transient Set<Map.Entry<K,V>> entrySet; 
transient int size; //map中存的KV对的个数
transient int modCount; //修改次数
int threshold; //阈值,当上面的size> threshold时,需对哈希表做扩容
final float loadFactor; //加载因子
```
modCount记录了Map新增/删除KV键值对,或者内部结构做了调整的次数,其主要作用,是对Map的iterator()操作做一致性校验,如果在iterator操作的过程中,
map的数值有修改,直接跑出ConcurrentModificationException异常。

threshold = table.length * loadFactor; 
判断size > threshold是否成立,这是决定哈希table是否扩容的重要因素。
#### put方法
put方法是有返回值,场景区分如下:
场景1:若执行put操作前,key已经存在,那么在执行put操作时,会使用本次的新value值来覆盖前一次的旧value值,返回的是旧value值。
场景2:若key不存在,则返回null值。

特殊key值处理:
>* HashMap中,是允许key、value都为null的,且key为null只存一份,多次存储会将旧value值覆盖。
>* key为null的存储位置,都统一放在下标为0的bucket,即table[0]位置的链表;
>* 如果是第一次对key=null做put操作,将会在table[0]的位置新增一个Entry节点,使用头插法做链表插入。

扩容:
>* 扩容后大小是扩容前的2倍。
>* 数据搬迁,从旧table迁移到扩容后的新table。为了避免碰撞过多,先决策是否需要对每个entry链表节点重新hash,然后根据hash值计算得到bucket
下标,然后使用头插法做节点迁移。

在计算hash值的时候,JDK7用了9次扰动处理=4次位运算+5次异或

如何计算bucket下标:
>* hash值的计算:首先得有key的hash值,计算方式使用了一种尽量减少碰撞的算式,使用key的hashCode作为算式的输入,得到了hash值。
>* 对于两个对象,若其hashCode相同,那么两个对象的hash值就一定相同。对于hashmap中key的类型,必须满足一下条件:如两个对象逻辑相等,
hashCode一定相等,反之却不一定成立。
>* 取模的逻辑: hashcode & (length - 1),得到哈希table的bucket下标。

#### 为什么非要将哈希table的大小控制为2的次幂数
>* 降低发生碰撞的概率,使散列更均匀。根据key的hash值计算bucket的下标位置时,使用与运算公式: hashCode & (length-1),
当还希表长度为2的次幂时,等同于使用表长度对hash值取模,散列更均匀。
>* 表的长度为2的次幂,那么(length-1)的二进制最后一位一定是1,在对hash值做与运算时,最后一位就可能是1,也可能是0。如果(leng-1)为
偶数,那么与元算后的值只能是0,奇数下标的bucket就永远散列不到,会浪费一半的空间。

#### 在目标bucket中遍历entry节点
通过hash值计算出下标,找到对应的目标bucket,然后对链表做遍历操作,逐个比较。
查找key的条件:e.hash == hash && ((k = e.key) == key || key.equals(k))

#### GET 方法
通过key的hash值计算目标bucket的下标,然后遍历对应bucket上的链表,逐个对比,得到结果。

#### Map中的迭代器Iterator
Map遍历的集中方式
方式1.Iterator迭代器
```
Iterator<Entry<String, Integer>> iterator = testMap.entrySet().iterator();
while (iterator.hasNext()) {
    Entry<String, Integer> next = iterator.next();
    System.out.println(next.getKey() + ":" + next.getValue());
}
```
方式2.entryset
```
for (Map.Entry<String, Integer> entry : testMap.entrySet()) {
    System.out.println(entry.getKey() + ":" + entry.getValue());
}
```
方式3.keyset
```
Map<String,Object> map = new HashMap<>();
for (String key:map.keySet()){
    System.out.println(key+":"+map.get(key));
}
```
方式4.foreach(不推荐使用,新增二次查询)
```
testMap.forEach((key, value) -> {
    System.out.println(key + ":" + value);
});
```
方式5.通过key的set集合遍历
```
Iterator<String> keyIterator = testMap.keySet().iterator();
while (keyIterator.hasNext()) {
    String key = keyIterator.next();
    System.out.println(key + ":" + testMap.get(key));
}
```
如果在使用的过程中，只需要key，使用方式5较为合适,如果只需要value,可以使用方式1。

#### Iterator的实现原理
Iterator是一个接口,拥有三个方法:
```
public interface Iterator<E> {
    boolean hasNext();
    E next();
    default void remove() {
        throw new UnsupportedOperationException("remove");
    }
}
```
在HashMap中,新增了一个内部抽象类HashIterator,在遍历的时候，以Entry节点进行遍历,EntryIterator(实现next方法)继承HashIterator。

#### fail-fast策略
fail-fast:尽可能早的发现问题,立即终止当前执行过程,有更高层级的系统来做处理。
在HashMap中，提交的modCount域变量,就是用于实现HashMap的fail-fast。
在Map做迭代的时候,会将modCount域变量赋值给expectedModCount局部变量。在迭代过程中,用于做内容修改次数的一致性校验。
若此时有其他线程或本线程的其他操作对此Map做了内容修改时,那么会导致modCount和expectedModCount不一致,立即抛出
ConcurrentModificationException。
在JDK7中,HashMap是一个并发不安全的问题，在迭代操作是采用的fail-fast机制;在并发添加操作中会出现丢失更新问题;因为采用头插法在并发扩容
时会产生环形链表的问题,导致CPU达到100%,甚至宕机。

解决并发问题可以采用:Collections.synchronizedMap()方法,返回一个线程安全的Map;CurrentHashMap(采用分段锁实现线程安全);HashTable(不推荐)

下面是删除元素的方法:
```
Iterator<Entry<String, Integer>> iterator = testMap.entrySet().iterator();
while (iterator.hasNext()) {
    Entry<String, Integer> next = iterator.next();
    System.out.println(next.getKey() + ":" + next.getValue());
    if (next.getKey().equals("s2")) {
        iterator.remove();
    }
}
```
在单线程中,没有问题;如果在多个线程,就会出现问题。

### JDK8的HashMap底层实现
HashMap在JDK8中对链表结构做了优化,在一定条件下将链表转为红黑树,提升查询效率。
JDK8之前采用数组(位桶)+链表的方式实现,通过链表处理冲突。

JDK8之后采用数组+链表+红黑树的方式实现,使用链表存储的优点是可以降低内存的使用率,而红黑树的优点是查询效率更高。

#### Put方法
在插入节点时,JDK7新增节点是使用头插法,JDK8中,在链表使用尾插法,将待新增的节点追加到链表末尾。

put方法执行过程:
>* 若哈希table为null,或长度为0，则做一次扩容操作。
>* 根据index找到目标bucket,若当前bucket上没有节点,那么直接新增一个节点，赋值给该bucket;
>* 若当前bucket上有链表,且头节点就匹配,那么直接做替换即可
>* 若当前bucket上的是树结构,则转为红黑树的插入操作
>* 如果上面的题哦件都不成立,则对链表做遍历操作:
>   * 若链表中有节点匹配,则做value替换;
>   * 若没有节点匹配,则在链表末尾追加。
>       * 若链表长度大于TREEIFY_THRESHOLD,执行红黑树转换操作
>       * 若第一个条件不成立,便直接扩容。
>* 如果上面的步骤都执行完之后,节点的数量超过阈值,再次扩容。

什么场景下会触发扩容？
>* 哈希table为null或长度为0;
>* Map中存储的KV数量超过threshold;
>* 链表中的长度超过TREEIFY_THRESHOLD,但表长度却小于MIN_TREEIFY_CAPACITY。

扩容的时候,JDK8只用了2次扰动处理=1次为运算+1次异或

#### GET方法
先根据key计算hash值,进一步计算得到哈希table的目标index,若此bucket上为红黑树,则再红黑树上查找,若不是红黑树,遍历链表。


#### HashMap的JDK8、JDK7的区别
|版本|JDK8|JDK7|
|----|----|----|
|扩容|前置条件:插入键值对时,发现容量不足;尾插法;扩容结束:2倍的哈希表 & 转移旧数据的到新table|前置条件:插入键值对时,发现容量不足;头插法;扩容结束:2倍的哈希表 & 转移旧数据的到新table;后置条件:重新计算hash、存储位置|
|区别:扩容后存储位置的计算方式|按照扩容后的规律计算(扩容后的位置=原位置 or 原位置 + 旧容量)|全部按照原来方法进行计算(hashCode->扰动处理->(h&length-1)|
|区别:转移数据方式|尾插法(直接插入到链表尾部/红黑树,不会出现逆序&环形链表死循环问题)|头插法(先将原位置的数据移到后1位,在插入数据到该位置;会出现逆序&环形链表死循环问题)|
|区别:需要插入数据的时机&位置|扩容前插入、转移数据时统一计算|扩容后插入、单独计算|

JDK7用的是头插法，而JDK8及之后使用的都是尾插法，那么他们为什么要这样做呢？因为JDK1.7是用单链表进行的纵向延伸，
当采用头插法时会容易出现逆序且环形链表死循环问题。但是在JDK1.8之后是因为加入了红黑树使用尾插法，能够避免出现逆序且链表死循环的问题。

扩容后数据存储位置的计算方式也不一样：1. 在JDK1.7的时候是直接用hash值和需要扩容的二进制数进行&（
这里就是为什么扩容的时候为啥一定必须是2的多少次幂的原因所在，因为如果只有2的n次幂的情况时最后一位二进制数才一定是1，这样能最大程度减少hash碰撞）
（hash值 & length-1）

JDK7的时候使用的是数组+单链表的数据结构。但是在JDK8及之后时,使用的是数组+链表+红黑树的数据结构(当链表的深度达到8的时候,
也就是默认阈值,就会自动扩容把链表转成红黑树的数据结构来把时间复杂度从O(n)变成O(logN)提高了效率)。





## HashMap、HashTable是什么关系

### 共同点与异同点 

共同点:  底层都是使用哈希表+链表的实现方式。

区别:
>* 从层级结构上看,HashMap、HashTable有一个公用的Map接口,另外,HashTable还单独继承了一个抽象类Dictionary;
>* HashTable诞生自JDK1.0,HashMap从JDK1.2之后才有;
>* HashTable线程安全,HashMap线程不安全;
>* 初始值和扩容方式不同。HashTable的初始值为11,扩容为原先大小的2*d+1。容量大小都采用奇数为素数,且采用取模法,这种方式散列更均匀。
但有个缺点就是对素数取模的性能较低,而HashTable的长度都是2的次幂,这种方式的取模都是直接做为运算,性能较好。
>* HashMap的key、value都可为null,且value可多次为null,key多次为null时会覆盖。当HashTable的Key、value都不可为null,
否则直接NullPointException。

HashTable计算索引为:index = (hash &0x7FFFFFFF) % tab.length;


### HashMap的线程安全
HashTable中的put操作、get操作、remove操作、equals操作，都使用了synchronized关键字修饰。
但是,这样会导致效率低下。
因为synchronized的特性,对于多线程共享的临界资源,同一时刻只能有一个线程占用,其他线程必须原地等待,大量线程必须排队一个个处理。
get方法问什么必须添加synchronized:
如果A线程在做put或者remove操作时,B线程或者C线程此时都可以同时执行get操作,可能哈希table已经被被A线程改变,也会带来问题,因此不加不行。


## HashMap线程不安全在那

### 线程覆盖问题
两个线程执行put、删除、修改操作时,可能导致数据覆盖。
在JDK7中,A,B两个线程同时执行put操作,且两个ke都指向同一个bucket,那么此时两个节点,都会做头插法。
当A线程和B线程都获取到了bucket的头节点后,若此时A线程的时间片用完,B线程将其新数据完成了头插法操作,
此时轮到A线程操作,但这时A线程所具有的旧头节点已经过时了,线程A再做头插法操作,就会抹掉B刚刚新增的节点,
导致数据丢失。

JDK7是用单链表进行的纵向延伸,当采用头插法时会容易出现逆序且形成链表死循环问题。
JDK8之后,加入了红黑树使用尾插法,能够避免出现逆序且链表死循环的问题。


### 扩容时导致死循环
在JDK7之前的版本会存在死循环现象;JDK8中,resize()方式已经做了调整,使用两队链表,且都是使用的尾插法,
及时多线程下,也顶多是从头节点在做一次尾插法,不会造成死循环。而JDK7能造成死循环,就是因为resize()时
使用了头插法,将原先的顺序做了反转,才留下了死循环的机会。

### 小结

多线程环境下使用HashMap,会引起各类问题,分为三类:
>* 死循环
>* 数据重复
>* 数据丢失

JDK5之前,多线程环境往往使用HashTable,JDK5之后,出现了ConcurrentHashMap类,采用分段锁来实现了线程安全,
相对HashTable有更多的性能,推荐使用

## 如何规避HashMap的线程不安全
### 将Map转为包装类
```
Map<String, Integer> testMap = new HashMap<>();
...
// 转为线程安全的map,在对象中添加对象锁
Map<String, Integer> map = Collections.synchronizedMap(testMap);
```
### 使用ConcurrentHashMap
JDK7版本以及以前,ConcurrentHashMap类使用了分段锁的技术,JDK8使用了synchronized修饰符。







## 参考文献
[HashMap面试题，看这一篇就够了！](https://www.lagou.com/lgeduarticle/78202.html)
[HashMap面试题，看这一篇就够了！](https://juejin.im/post/5de85e05f265da33b50727f6)
[【Java 容器面试题】谈谈你对HashMap 的理解](https://juejin.im/post/5c1da988f265da6143130ccc)
[（1）美团面试题：Hashmap的结构，1.7和1.8有哪些区别，史上最深入的分析](https://blog.csdn.net/qq_36520235/article/details/82417949)
[（5）美团面试题：HashMap是如何形成死循环的？（最完整的配图讲解）](https://blog.csdn.net/qq_36520235/article/details/86653136)
