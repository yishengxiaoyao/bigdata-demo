# Java集合框架
## Java集合类
Set:无序、不可重复;List有序、重复的集合;Queue代表队列集合实现;Map代表具有映射关系的集合。
### Java集合与数组的区别
Java 集合与数组的区别:
>* 数组的长度是不可变的,不能动态添加数据;集合可以保存不确定数量的数组，同时也可以保存具有映射关系的数据。
>* 同一个数组的元素即基本类型的值,也可以是对象;集合只能存储同一类型的对象。

### Java集合介绍
Set的元素是不可重复，不能存在相同的对象，可以存放不能的对象，具有相同的值。
List中可以存储相同的对象，按照顺序存储。
Queue用于模拟队列，不能随机访问。

Map的key是一个set集合,不能重复(keySet());Map的value是一个list集合,可以重复,使用list存储(values)。

## ArrayList
List是一个接口,它继承于Collection接口,代表有序的队列。
AbstractList是一个抽象类,它继承于AbstractCollection。
AbstractSequentialList是一个抽象类,实现了链表中,根据index索引值操作链表的全部方法。
ArrayList、LinkedList、Vector和Stack是List的四个实现类。
Vector是基于数组实现的,是矢量队列,和ArrayList一样,也是一个动态数组,有数组实现。ArrayList是非线程安全的,Vector是线程安全的。
Stack是基于数组实现的,具有Vector先进后出特性。
ArrayList是基于数组实现的List类,内部封装一个动态的、允许在分配的数组，数组可以动态增长。
ArrayList是线程不安全,可以通过Collections.synchronizedList(List l)返回一个线程安全的ArrayList集合。
ArrayList实现了RandomAccess接口,因此具有随机访问功能,通过数组的下标进行快速访问;实现Cloneable接口,能被克隆。
ArrayList默认的大小为10，可以动态扩展，每次扩展为1.5倍，Vector是扩展为两倍。
ArrayList的add和remove方法使用到System.arrayCopy()方法。ArrayList允许存在null。

## LinkedList
LinkedList是双链表结构，实现了List和Queue接口，允许元素为null。
LinkedList是非线程安全的。可以通过Collections.synchronizedList(new LinkedList(...))创建线程安全的List。
LinkedList实现了Closeable接口,能被克隆。

header是双向链头的表头，表头的节点对象是个Node类实例(在JDK7之前是Entry)

## Synchronized
### Synchronized 简介
synchronized实现同步的基础:java中每个对象都可以作为锁对象。当线程试图访问同步代码时,必须获取对象锁,退出或者跑出异常时必须释放锁,否则线程会一直处于阻塞状态。
synchronized实现同步的表现形式分为两种:同步代码块和同步方法。

### synchronized同理
同步代码块:任何一个对象都有一个监视器与之关联,线程执行监视器指令时,会尝试获取对象对应的监视器的所有权,即尝试获得对象的锁。
同步方法:使用synchronized关键字修饰的方法,称之为同步方法。
两个的本质都是对一个对象的监视器的获取。任意一个对象都拥有自己的监视器。当同步代码块或同步方法时,执行方法的线程必须先获取该对象的监视器才能
进入同步代码或同步方法,没有获取监视器的线程将会被阻塞,并进入同步队列,线程状态编程阻塞状态。当成功获取监视器的线程释放了锁后,会唤醒在阻塞同步
队列的线程,使其重新尝试对监视器的获取。

synchronized特点:
>* 当前线程的同步方法、同步代码块执行结束,当前线程即释放同步监视器。
>* 当线程在同步代码块、同步方法中遇到break、return终止了该代码块、该方法继续执行,当前线程会释放同步监视器。
>* 当前线程在同步代码块或同步方法中出现了未处理的Error或Exception,导致了代码的异常终止,此时线程的同步监视器也会被释放。
>* 当前线程在执行同步代码块或同步方法时,执行了同步监视器对象的wait方法，导致当前线程的停止,此时会释放监视器。

在执行同步方法或同步代码块,调用thread.sleep()、yield()方法来暂停线程、此线程不会释放监视器。


## ReentrantLock
synchronized 无法中断一个正在等候获得锁的线程，也无法通过投票得到锁
ReentrantLock 拥有与 synchronized 相同的并发性和内存语义，但是添加了类似锁投票、定时锁等候和可中断锁等候的一些特性.

JDK5之前,靠synchronized关键字来实现锁功能,处理多线程并发的问题;在JDK5之后新增了lock来实现锁的功能,同时也提供了ReentrantLock实现类。
ReentrantLock使用时需要显式的获取或释放锁，而synchronized可以隐式获取和释放锁，也就是说，在正常使用情况下，ReentrantLock需要手动操作锁的获取和释放，synchronized可以自动的获取和释放，从操作性上synchronized是相对便捷的，居然ReentrantLock是手动的，那么也有它的优势，就是可以自定义一些其他的操作，比如中断锁的获取及超时获取锁等多种特性。

Lock接口的一些方法:
void lock():如果锁处于空闲状态,当前线程获取到锁。相反,如果锁已经被其他线程持有,将仅有当前线程,直到当前线程获取到锁。
boolean tryLock():如果锁可用,则获取锁,并立即返回true。否则返回false,tryLock()只是试图获取锁,如果锁不可用,不会导致当前线程被禁用,
当前线程仍然继续往下执行代码。lock()方法一定要获取到锁,如果锁不可用,就一直等待,在未获取锁之前,当前线程并不继续向下执行,通常采用如下的
代码形式调用tryLock()方法。
void unlock():当前线程将释放持有的锁,锁只能有持有者释放,如果线程并不持有锁,却执行该方法,可能导致异常的发生。
Condition newCondition():条件对象,获取等待通知组件,该组件和当前的锁绑定,当前线程直邮获取了锁,才能调用该组件的await()方法,而调用后,当前线程将释放锁。

synchronized控制的锁是非公平锁。这种非公平现象,有可能造成一些线程都无法获取CPU资源的执行权,而优先级高的线程会不断增加自己执行资源。
要解决这种饥饿非公平问题,需要引入公平锁。

公平锁:可以保证线程的执行顺序,可以避免非公平现象的产生,但效率比较低,因为要执行顺序执行,需要维护一个有序队列。
公平锁的实现,只需在ReentrantLock的构造函数传入true即可,false则是非公平锁,无参构造函数默认是false。

## Synchronized和ReentrantLock的比较
>* Lock一个接口,提供ReentrantLock实现类,JDK实现;而synchronized是个关键字,是java内置线程同步,是JVM实现。
>* synchronized在发生异常时,会自动的释放线程占用锁对象,不会死锁的现象发生,而Lock在发生异常时,如果没有主动的通过unlock方法释放对象,
则可能会造成死锁的发生,因此在使用Lock时需要在finally块中释放锁。
>* Lock可以让等待锁的线程中断,而synchronized不行,会一直等待下去，直到有唤醒的操作。
>* Lock可以判断线程是否获取锁对象,而synchronized则不行。
>* synchronized中的锁是非公平的,ReentrantLock默认情况下是非公平的,但是也可以是公平的。
>* 一个ReentrantLock可以同时绑定多个Condition对象。
>* synchronized和ReentrantLock都是可重入锁。


当竟争资源非常激烈时,此时ReentrantLock的性能要远远优于synchronized。
JDK5 synchronized是性能低效的,是阻塞的实现,JDK6的基础上,对synchronized进行优化。



## CopyOnWriteArrayList
CopyOnWrite容器即写时复制的容器。通俗的理解是当我们往一个容器添加元素的时候，不直接往当前容器添加，而是先将当前容器进行Copy，
复制出一个新的容器，然后新的容器里添加元素，添加完元素之后，再将原容器的引用指向新的容器。
这样做的好处是我们可以对CopyOnWrite容器进行并发的读，而不需要加锁，因为当前容器不会添加任何元素。
所以CopyOnWrite容器也是一种读写分离的思想，读和写不同的容器。

CopyOnWrite并发容器用于读多写少的并发场景。例如白名单、黑名单、商品类目的访问和更新场景。

CopyOnWriteArrayList不支持迭代的时候对容器进行修改，而ArrayList本身的迭代器是支持迭代中更改容器结构。
CopyOnWriteArrayList 底层结构为:数组;读取结构,无锁;修改列表,加锁,确保始终只有一个线程在修改列表内容。
每次修改都会先上锁,然后进行数组拷贝,性能较ArrayList低,读取无锁,读的性能比Vector高;
遍历时,是对列表中当前所指向的数组进行遍历,遍历过程中对数组的修改,不会影响遍历的内容。
使用显示锁ReentrantLock来加锁所有的写操作，实现线程安全。
由于array属性被volatile修饰,添加完成后,其他线程就可以立刻查看看到被修改的内容。
CopyOnWriteArrayList保证数据在多线程操作时的最终一致性。

HashTable、Vector加锁的粒度大(直接在方法声明处使用synchronized)。
ConcurrentHashMap(ConcurrentHashMap用了CAS锁)、CopyOnWriteArrayList加锁粒度小。
JUC下的线程安全容器在遍历的时候不会跑出ConcurrentModificationException异常。


## CopyOnWrite vs 读写锁
相同点:1.两者都是通过读写分离的思想实现;2.读线程间是互不阻塞的。
不同点:对读线程而言,为了实现数据实时性,在写锁被获取后,读线程会等待或者当读锁被获取后,写线程会等待,从而解决脏读等问题。即使用读写锁依然会出现读线程阻塞等待的情况。
COW则完全放开了牺牲数据实时性而保证数据最终一致性,即读线程对数据的更新是延迟感知的,因此读线程不会存在等待的情况。


COW的缺点:
>* 内存占用问题: 进行写操作的时候,内存里会同时驻扎两个对象的内存,旧的对象和新写入的对象(写的时候,旧容器的对象还在使用,两份对象内存)。
如果内存较少,会频繁发生minor GC 和major GC。
>* 数据一致性问题:COW容器只能保证数据的最终一致性，不能保证数据的实时一致性。

## ArrayList 与 LinkedList的性能区别
### 结构差别
|List|存储结构|特点|循环时间复杂度|get(i)时间复杂度|总时间复杂度|实现|
|----|----|----|----|----|----|----|
|ArrayList|数组结构|可以根据下标直接取值|O(n)|O(1)|O(n)|基于数组实现,可动态扩容数组的容量|
|LinkedList|链表结构|如果需要寻找某一个下标的数值必须从头遍历|O(n)|O(n)|O(n^2)|基于双向链表的实现,可以做堆栈、队列使用|

ArrayList在随机访问set和get比LinkedList的效率更高,因为LinkedList要通过遍历查询遍历移动指针,而ArrayList只需通过index在数组取出即可。
在末尾添加元素，ArrayList比LinkedList更高效。在首位添加元素，LinkedList比ArrayList高效(ArrayList使用System.arrayCopy()移动元素)。

LinkedList不支持高效的随机元素访问。

|类别|ArrayList|Vector|LinkedList|
|----|----|----|----|
|优点|适合查找|适合查找|不适合查找|
|继承类|AbstractList|AbstractList|AbstractSequentialList|
|实现接口|List,RandomAccess,Cloneable,Serializable|List,RandomAccess,Cloneable,Serializable|List,Deque,Cloneable,Serializable|
|线程安全|否|是|否|
|数组增量|50%|100%||
|数据结构|数组|数组|双向链表|
|适用场景|适用于需要频繁查找元素的场景|适用于需要频繁查找元素的场景|适用于需要频繁插入删除元素的场景|

LinkedList双向列表的实现也比较简单，通过计数索引值实现，从链表长度的1/2开始查找，下标大了就从表头开始找，小了就从表尾开始找。

ArrayList创建底层数组时,JDK7为饿汉式，JDK8是懒汉式。

ArrayList用for循环遍历比iterator迭代器遍历快，LinkedList用iterator迭代器遍历比for循环遍历快，
因为ArrayList继承自RandomAccess。

### 性能测试
在遍历ArrayList或者LinkedList，需要使用Iterator或者foreach。
```
# iterator例子
public void iteratorList(List<Integer> lists){
    Iterator<Integer> it = lists.iterator();
    while (it.hasNext()){
        Integer integer = it.next();
        // TODO 处理数据
    }
}
```
```
# foreach 例子
public void foreachList(List<Integer> lists){
    for (Integer i : lists) {
        // TODO 处理数据
    }
}
```
list在遍历删除元素时，需要使用iterator进行遍历删除。对CopyOnWriteArrayList中元素进行遍历删除，需要使用for循环。
## 
## 参考文献
[ArrayList与LinkedList遍历性能比较](https://www.gcssloop.com/tips/arratlist-linkedlist-performance)
[Java集合框架（一）](https://blog.csdn.net/hzw2017/article/details/80294091)
[Java集合框架之ArrayList、LinkedList的区别（四）](https://blog.csdn.net/hzw2017/article/details/80375035)
[源码浅析ArrayList、LinkedList和Vector的区别](https://blog.csdn.net/u012814441/article/details/80671604)
[美团试题：ArrayList和linkedlist有什么区别，如何遍历，使用for循环遍历linkedlist为什么效率低，linkedlist能使用索引访问么，使用迭代器呢](https://blog.csdn.net/qq_36520235/article/details/82535044)
