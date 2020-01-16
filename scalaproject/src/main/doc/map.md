# HashTable vs HashMap vs ConcurrentHashMap

## HashMap
### JDK7 中的HashMap底层实现
#### 基础知识
HashMap继承AbstractMap,实现Map。不管JDK7,还是JDK8,HashMap的实现框架都是哈希表+链表的组合方式。
```
static final int DEFAULT_INITIAL_CAPACITY = 1 << 4; // aka 16 初始大小,必须为2的次幂
static final int MAXIMUM_CAPACITY = 1 << 30; //哈希表的最大长度
static final float DEFAULT_LOAD_FACTOR = 0.75f; //默认加载因子,如果没有指定,默认值
static final int TREEIFY_THRESHOLD = 8; //树形化的阈值,JDK8
static final int UNTREEIFY_THRESHOLD = 6; //不树形化的阈值 JDK8
static final int MIN_TREEIFY_CAPACITY = 64; //最小树形化的值 JDK8
transient Node<K,V>[] table; //哈希表,存储Entry对象的数组
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

如何计算bucket下标:
>* hash值的计算:首先得有key的hash值,计算方式使用了一种尽量减少碰撞的算式,使用key的hashCode作为算式的输入,得到了hash值。
>* 