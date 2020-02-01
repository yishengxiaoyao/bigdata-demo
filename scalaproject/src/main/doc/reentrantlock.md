# ReentrantLock源码解析
ReentrantLock重入锁，是实现Lock接口的一个类，也是在实际编程中使用频率很高的一个锁，支持重入性，表示能够对共享资源能够重复加锁，
即当前线程获取该锁再次获取不会被阻塞。在java关键字synchronized隐式支持重入性，synchronized通过获取自增，释放自减的方式实现重入。
与此同时，ReentrantLock还支持公平锁和非公平锁两种方式。

## ReentrantLock属性
```
//同步器提供全部实现机制
private final Sync sync;
```
ReentrantLock只有一个属性,被private final修饰,不能被修改,并且是锁的全部实现。
各种锁的实现都有一个sync属性，并且是继承AbstractQueuedSynchronizer。
## ReentrantLock构造函数
```
//默认为非公平锁
public ReentrantLock() {
    sync = new NonfairSync();
}
//如果将参数设置为true,将创建公平锁
public ReentrantLock(boolean fair) {
    sync = fair ? new FairSync() : new NonfairSync();
}
```
NofairSync和FairSync都是继承自Sync,Sync是继承自AbstractQueuedSynchronizer。

## Sync
Sync没有自己的属性,提供了一个抽象方法，然后在NofairSync和FairSync提供自己的实现，具体的实现会在后面介绍。

## 公平锁
在AbstractQueuedSynchronizer中有一个属性:
```
//表示资源的可用状态
private volatile int state;
```
访问state的三种方式:
```
getState()、setState()、compareAndSetState()
```

AQS定义两种资源共享方式:
>* Exclusive独占:只有一个线程能执行,例如ReentrantLock
>* Share共享:多个线程共同执行,例如Semaphore/CountDownLatch。




### 单线程

#### 加锁过程
本文将根据后面提供的代码为解释的依据。
需要使用ReentrantLock带参数的构造函数来创建实例。
代码的输出为下面的内容:
```
Administrator Thread:-->Thread:xiaoyaostart
Thread-->Thread:xiaoyao first lock
Thread:Thread:xiaoyao 1 do something
Thread-->Thread:xiaoyao second lock
Thread:Thread:xiaoyao 2 do something
Thread:Thread:xiaoyao release lock
Thread:Thread:xiaoyao release lock
```
在调用锁的时候,会调用FairSync的lock方法。
```
//ReentrantLock.FairSync
final void lock() {
    //获取锁
    acquire(1);
}
//AbstractQueuedSynchronizer.java
public final void acquire(int arg) {
    //tryAcquire(arg)尝试加锁，如果加锁失败则会调用acquireQueued方法加入队列去排队，如果加锁成功则不会调用
    //加入队列之后线程会立马park，等到解锁之后会被unpark，醒来之后判断自己是否被打断了；被打断下次分析
   if (!tryAcquire(arg) &&
        acquireQueued(addWaiter(Node.EXCLUSIVE), arg))
        selfInterrupt();
}
//ReentrantLock.FairSync
protected final boolean tryAcquire(int acquires) {
        //获取当前线程
        final Thread current = Thread.currentThread();
        //获取状态机的值,从AbstractQueuedSynchonizer的变量，如果锁是自由状态则=0，如果被上锁则为1，大于1表示重入锁。
        int c = getState();
        //如果是首次申请锁
        if (c == 0) {
            //查询在获取锁的时候,判断是否有其他线程比当前线程等待的时间还要长
            //对状态进行同步更新设置
            if (!hasQueuedPredecessors() &&
                compareAndSetState(0, acquires)) {
                //将当前线程设置为独占拥有线程
                setExclusiveOwnerThread(current);
                return true;
            }
        }
        //再次申请锁,判断这个进程是否为独占进程
        //如果C不等于0，而且当前线程不等于拥有锁的线程则不会进else if 直接返回false，加锁失败
        //如果C不等于0，但是当前线程等于拥有锁的线程则表示这是一次重入，那么直接把状态+1表示重入次数+1
        //那么这里也侧面说明了reentrantlock是可以重入的，因为如果是重入也返回true，也能lock成功
        else if (current == getExclusiveOwnerThread()) {
            int nextc = c + acquires;
            if (nextc < 0)
                throw new Error("Maximum lock count exceeded");
            //更新同步状态
            setState(nextc);
            return true;
        }
        return false;
    }
}

public final boolean hasQueuedPredecessors() {
    /**
    * 不需要排队分两种情况:
    * 1.队列没有初始化。不需要排队,直接去加锁,可能会失败,如果是多个线程同时加锁,都认为没有初始化,都去进行CAS修改计数器;只有一个成功,其他的线程失败。
    * 2.队列被初始化。线程过来加锁,如果队列中第一个排队的节点是自己,则为重入锁。
    * 因为队列中第一个排队的线程会去尝试一下获取锁,因为有可能其他线程获取的锁已经释放了锁;
    * 如果释放了锁就直接获取锁就行。如果没有获取锁就去排队。
    * h != t 判断首尾不想等的三种情况:
    * 1.队列没有初始化。h和t都指向同一个节点,就是返回false,因为前面的判断是取反,直接通过cas获取锁。
    * 队列没有初始化,直接上锁;
    * 2.队列被初始化了。h != t，返回true,还需要判断后面的条件，
    * 如果队列中的节点大雨1个,然后将会(s = h.next) == null 为false,就需要判断后面的判断条件: s.thread != Thread.currentThread(),
    *  2.1  s.thread != Thread.currentThread() 为true。当前线程不能于排队的第一个线程s,就需要去排队
    *  2.2  s.thread != Thread.currentThread() 为false。 表示当前等于队列的第一个线程s,不需要去排队。
    *   2.2.1第一种情况加锁成功？有人会问为什么会成功啊，如这个时候h也就是持有锁的那个线程执行完了释放锁了，那么肯定成功啊；成功则执行 setExclusiveOwnerThread(current); 然后返回true 自己看代码
    *   2.2.2第二种情况加锁失败？有人会问为什么会失败啊。假如这个时候h也就是持有锁的那个线程没执行完没释放锁，那么肯定失败啊；失败则直接返回false
    * 如果队列被初始化了，而且至少有一个人在排队那么自己(男/女朋友)也去排队,就去获取锁;如果获取到锁,就直接执行操作,如果没有获取的话,直接去排队。
    * 3.队列被初始化了,只有一个数据节点。因为队列初始化的时候，会虚拟一个h作为头节点，因为AQS认为h永远是不排队的,
    * 第三种情况总结：如果队列当中只有一个节点，这个节点就是当前持有锁的那个节点，故而我不需要排队，进行cas；尝试加锁,这是AQS的设计原理，他会判断你入队之前，队列里面有没有人排队；
    * 没有人排队分两种情况；队列没有初始化，不需要排队队列初始化了，按时只有一个节点，也是没人排队，自己先也不排队只要认定自己不需要排队，则先尝试加锁；加锁失败之后再排队；
    * 再一次解释了不需要排队这个词的歧义性果加锁失败了，在去park，下文有详细解释这样设计源码和原因果持有锁的线程释放了锁，那么我能成功上锁
    */
    Node t = tail; // Read fields in reverse initialization order
    Node h = head;
    Node s;
    return h != t &&
        ((s = h.next) == null || s.thread != Thread.currentThread());
}
//addWaiter方法
private Node addWaiter(Node mode) {
    //由于AQS队列当中的元素类型为Node，故而需要把当前线程tc封装成为一个Node对象
    Node node = new Node(Thread.currentThread(), mode);
    //tail为对尾，赋值给pred 
    Node pred = tail;
    //判断pred是否为空，其实就是判断对尾是否有节点，其实只要队列被初始化了对尾肯定不为空，
    //假设队列里面只有一个元素，那么对尾和对首都是这个元素
    //换言之就是判断队列有没有初始化
    //上面我们说过代码执行到这里有两种情况，1、队列没有初始化和2、队列已经初始化了
    //pred不等于空表示第二种情况，队列被初始化了，如果是第二种情况那比较简单
   //直接把当前线程封装的nc的上一个节点设置成为pred即原来的对尾
   //继而把pred的下一个节点设置为当nc，这个nc自己成为对尾了
    if (pred != null) {
        //直接把当前线程封装的nc的上一个节点设置成为pred即原来的对尾
        node.prev = pred;
        //这里需要cas，因为防止多个线程加锁，确保nc入队的时候是原子操作
        if (compareAndSetTail(pred, node)) {
            //继而把pred的下一个节点设置为当nc，这个nc自己成为对尾了
            pred.next = node;
            //然后把nc返回出去，方法结束
            return node;
        }
    }
    //如果上面的if不成了就会执行到这里，表示第一种情况队列并没有初始化---下面解析这个方法
    enq(node);
    //返回nc
    return node;
}


private Node enq(final Node node) {//这里的node就是当前线程封装的node也就是nc
    //死循环
    for (;;) {
        //对尾复制给t，上面已经说过队列没有初始化，
        //故而第一次循环t==null（因为是死循环，因此强调第一次，后面可能还有第二次、第三次，每次t的情况肯定不同）
        Node t = tail;
        //第一次循环成了成立
        if (t == null) { // Must initialize
            //new Node就是实例化一个Node对象下文我们称为nn，
            //调用无参构造方法实例化出来的Node里面三个属性都为null，可以关联Node类的结构，
            //compareAndSetHead入队操作；把这个nn设置成为队列当中的头部，cas防止多线程、确保原子操作；
            //记住这个时候队列当中只有一个，即nn
            if (compareAndSetHead(new Node()))
                //这个时候AQS队列当中只有一个元素，即头部=nn，所以为了确保队列的完整，设置头部等于尾部，即nn即是头也是尾
                //然后第一次循环结束；接着执行第二次循环，第二次循环代码我写在了下面，接着往下看就行
                tail = head;
        } else {
            node.prev = t;
            if (compareAndSetTail(t, node)) {
                t.next = node;
                return t;
            }
        }
    }
}


//为了方便 第二次循环我再贴一次代码来对第二遍循环解释
private Node enq(final Node node) {//这里的node就是当前线程封装的node也就是nc
    //死循环
    for (;;) {
        //对尾复制给t，由于第二次循环，故而tail==nn，即new出来的那个node
        Node t = tail;
        //第二次循环不成立
        if (t == null) { // Must initialize
            if (compareAndSetHead(new Node()))
                tail = head;
        } else {
            //不成立故而进入else
            //首先把nc，当前线程所代表的的node的上一个节点改变为nn，因为这个时候nc需要入队，入队的时候需要把关系维护好
            //所谓的维护关系就是形成链表，nc的上一个节点只能为nn，这个很好理解
            node.prev = t;
            //入队操作--把nc设置为对尾，对首是nn，
            if (compareAndSetTail(t, node)) {
                //上面我们说了为了维护关系把nc的上一个节点设置为nn
                //这里同样为了维护关系，把nn的下一个节点设置为nc
                t.next = node;
                //然后返回t，即nn，死循环结束，enq(node);方法返回
                //这个返回其实就是为了终止循环，返回出去的t，没有意义
                return t;
            }
        }
    }
}

  //这个方法已经解释完成了
  enq(node);
  //返回nc，不管哪种情况都会返回nc；到此addWaiter方法解释完成
  return node;

总结：addWaiter方法就是让nc入队-并且维护队列的链表关系，但是由于情况复杂做了不同处理
主要针对队列是否有初始化，没有初始化则new一个新的Node nn作为对首，nn里面的线程为null
接下来分析acquireQueued方法
```

#### 解锁过程
```
//释放锁
public final boolean release(int arg) {
    if (tryRelease(arg)) {
        Node h = head;
        if (h != null && h.waitStatus != 0)
            unparkSuccessor(h);
        return true;
    }
    return false;
}
//获取当前重入锁的状态数量,当线程要释放锁的时候,如果不是当前线程来执行这个操作,抛出异常,如果是的话,更新状态的值
protected final boolean tryRelease(int releases) {
    int c = getState() - releases;
    if (Thread.currentThread() != getExclusiveOwnerThread())
        throw new IllegalMonitorStateException();
    boolean free = false;
    if (c == 0) {
        //最后一个锁被释放的时候,将独占进程设置为空
        free = true;
        setExclusiveOwnerThread(null);
    }
    setState(c);
    return free;
}

final boolean acquireQueued(final Node node, int arg) {//这里的node 就是当前线程封装的那个node 下文叫做nc
    //记住标志很重要
    boolean failed = true;
    try {
        //同样是一个标志
        boolean interrupted = false;
        //死循环
        for (;;) {
            //获取nc的上一个节点，有两种情况；1、上一个节点为头部；2上一个节点不为头部
            final Node p = node.predecessor();
            //如果nc的上一个节点为头部，则表示nc为队列当中的第二个元素，为队列当中的第一个排队的Node；
            //如果nc为队列当中的第二个元素，第一个排队的则调用tryAcquire去尝试加锁---关于tryAcquire看上面的分析
            //只有nc为第二个元素；第一个排队的情况下才会尝试加锁，其他情况直接去park了，
            //因为第一个排队的执行到这里的时候需要看看持有有锁的线程有没有释放锁，释放了就轮到我了，就不park了
            //有人会疑惑说开始调用tryAcquire加锁失败了（需要排队），这里为什么还要进行tryAcquire不是重复了吗？
            //其实不然，因为第一次tryAcquire判断是否需要排队，如果需要排队，那么我就入队；
            //当我入队之后我发觉前面那个人就是第一个，持有锁的那个，那么我不死心，再次问问前面那个人搞完没有
            //如果他搞完了，我就不park，接着他搞我自己的事；如果他没有搞完，那么我则在队列当中去park，等待别人叫我
            if (p == head && tryAcquire(arg)) {
                //能够执行到这里表示我来加锁的时候，锁被持有了，我去排队，进到队列当中的时候发觉我前面那个人没有park，
                //前面那个人就是当前持有锁的那个人，那么我问问他搞完没有
                //能够进到这个里面就表示前面那个人搞完了；所以这里能执行到的几率比较小；但是在高并发的世界中这种情况真的需要考虑
                //如果我前面那个人搞完了，我nc得到锁了，那么前面那个人直接出队列，我自己则是对首；这行代码就是设置自己为对首
                setHead(node);
                //这里的P代表的就是刚刚搞完事的那个人，由于他的事情搞完了，要出队；怎么出队？把链表关系删除
                p.next = null; // help GC
                //设置表示---记住记加锁成功的时候为false
                failed = false;
                //返回false；为什么返回false？下次博客解释---比较复杂和加锁无关
                return interrupted;
            }
            //进到这里分为两种情况
            //1、nc的上一个节点不是头部，说白了，就是我去排队了，但是我上一个人不是队列第一个
            //2、第二种情况，我去排队了，发觉上一个节点是第一个，但是他还在搞事没有释放锁
            //不管哪种情况这个时候我都需要park，park之前我需要把上一个节点的状态改成park状态
            //这里比较难以理解为什么我需要去改变上一个节点的park状态呢？每个node都有一个状态，默认为0，表示无状态
            //-1表示在park；当时不能自己把自己改成-1状态？为什么呢？因为你得确定你自己park了才是能改为-1；
            //不然你自己改成自己为-1；但是改完之后你没有park那不就骗人？
            //你对外宣布自己是单身状态，但是实际和刘宏斌私下约会；这有点坑人
            //所以只能先park；在改状态；但是问题你自己都park了；完全释放CPU资源了，故而没有办法执行任何代码了，
            //所以只能别人来改；故而可以看到每次都是自己的后一个节点把自己改成-1状态
            if (shouldParkAfterFailedAcquire(p, node) &&
                //改上一个节点的状态成功之后；自己park；到此加锁过程说完了
                parkAndCheckInterrupt())
                interrupted = true;
        }
    } finally {
        if (failed)
            cancelAcquire(node);
    }
}
```

#### Node类
//标记节点为共享模式
static final Node SHARED = new Node();
//标记节点为独占模式
static final Node EXCLUSIVE = null;
//在同步队列中等待的线程等待超时或者被终端,需要从同步队列中取消等待
static final int CANCELLED = 1;
//后继节点处于等待状态,而当前的节点如果释放了同步状态或者取消,将会通知后继节点,使得后继节点的线程得以运行。
static final int SIGNAL = -1;
//节点在等待队列中,节点的线程等待在Condition上,当其他线程对Condition调用了signal()方法后,该节点会从等待队列中移动到同步队列中,加入到同步状态的获取
static final int CONDIDITION = -2;
//表示下一次共享同步状态将会被无条件地传播下去
static final int PROPAGATE = -3;
//标记当前节点的信号量状态,只能是(1,0,-1,-2,-3)的一种状态;使用CAS更改状态,volatile可以保证线程可见性,高并发场景下,即使一个线程修改之后,状态立马就会被其他线程看到
volatile int waitStatus;
//前驱节点,当前节点加入到同步队列中被设置
volatile Node prev;
//后继节点
volatile Node next;
//节点同步状态的线程。
volatile Thread thread;

### 多线程

#### 加锁过程

#### 解锁过程



## AQS

Java并发包当中的大多数同步器实现都是围绕着共同的基础行为,例如等待队列、条件队列、独占获取、共享获取等。而这个行为的抽象就是基于AbstractQueuedSynchronizer简称AQS。
AQS是一个抽象同步框架,可以用来实现一个依赖状态的同步器。

AQS具备的特性:
>* 阻塞等待队列
>* 共享/独占
>* 公平/非公平
>* 可重入
>* 允许中断




## 自旋锁


## 代码
```
public class LockTemplate {
    private Integer counter = 0;
    /**
     * 可重入锁、公平锁
     * 公平锁:需要保证多个线程使用的是同一个锁
     */
    private ReentrantLock lock = new ReentrantLock();

    /**
     *  需要保证多个线程使用同一个ReentrantLock对象
     * @param threadName
     */
    public void modifyResources(String threadName){
        System.out.println("Administrator Thread:-->"+threadName+"start");
        lock.lock();
        System.out.println("Thread-->"+threadName+" first lock");
        counter++;
        System.out.println("Thread:"+threadName+" "+counter+" do something");
        lock.lock();
        System.out.println("Thread-->"+threadName+" second lock");
        counter++;
        System.out.println("Thread:"+threadName+" "+counter+" do something");
        lock.unlock();
        System.out.println("Thread:"+threadName+" release lock");
        lock.unlock();
        System.out.println("Thread:"+threadName+" release lock");
    }
    public static void main(String[] args) {

        LockTemplate tp= new LockTemplate();
        new Thread(()->{
            String threadName = Thread.currentThread().getName();
            tp.modifyResources(threadName);
        },"Thread:xiaoyao").start();

        /*new Thread(()->{
            String threadName = Thread.currentThread().getName();
            tp.modifyResources(threadName);
        },"Thread:yishengxiaoyao").start();*/
    }
}
```

## 参考文献
[JUC AQS ReentrantLock源码分析（一）](https://blog.csdn.net/java_lyvee/article/details/98966684)