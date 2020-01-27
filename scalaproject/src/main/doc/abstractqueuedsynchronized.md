# AbstractQueuedSynchronized源码剖析
AbstractQueuedSynchronized是一个抽象同步框架,用来实现一个依赖状态的同步锁。
## AbstractQueuedSynchronized具备特性
>* 阻塞等待队列
>* 共享/独占
>* 公平/非公平
>* 可重入
>* 允许终端

JVM提供了synchronized(内置锁)关键字,保证并发安全
Java实现的AbstractQueuedSynchronized。

Java代码实现的锁都是显示锁

JUC分类:
>* locks部分:显示锁(互斥锁和读写锁)相关;
>* atmoic部分:原子变量类相关,是构建非阻塞算法的基础;
>* executor部分:线程池相关;
>* collections部分:并发容器相关
>* tools:同步工具相关,例如信号量、闭锁、栅栏等功能

synchronized是可重入的锁,针对对象进行加锁。

AQS:



AbstractQueuedSynchronized继承自AbstractOwnableSynchronizer。
AbstractOwnableSynchronizer 有一段描述:
```
同步器是需要被线程互斥访问的。AOS提供了创建锁并且赋予锁的权限的概念。AOS即不管理也不使用这些信息。AOS的子类或者其他工具
在合适的时候去维护这些信息用来控制和监控访问控制权。
```
AQS是基于模板模式的设计。

AbstractQueuedSynchronizer的解释:
```
为实现阻塞锁和同步器提供框架,这个同步器是依赖于先进先出的队列的。这个类别设计为依赖与单个原子int值来表示状态,是大多数同步器的基础。
子类必须定义改变此状态的protect方法,并且定义该状态对于获取或释放对象的含义。类中的其他方法将执行所有队列和阻塞机制。
子类必须维护其他状态字段,但是只有使用方法getState,setState和compareAndSetState操作的原子更新的int值才会跟踪同步。
```
AQS使用一个int型的字段来表示同步状态;AQS依赖FIFO等待队列来完成线程的排队工作。

AbstractQueuedSynchronizer定义了一些变量:
```
//等待队列的头节点,延迟初始化.除了初始化,它只能通过setHead方法修改.如果头节点存在,头节点的状态不能是CANCELLED
private transient volatile Node head;
//等待队列的尾节点,延迟初始化。只能通过enqf方法来加载新的等待节点
private transient volatile Node tail;
//同步状态
private volatile int state;
```
队列的对头、队尾节点和同步状态都用volatile状态,保证了多线程之间的可见性。节点类的实现:
```
static final class Node {
    //标记以表明节点正在共享模式等待
    static final Node SHARED = new Node();
    //标记表明节点正在以独占锁模式等待
    static final Node EXCLUSIVE = null;
    //waitStatus值表明线程已经取消
    static final int CANCELLED =  1;
    //waitStatus值表明后续线程需要释放
    static final int SIGNAL    = -1;
    //waitStatus值表明线程正在等待条件
    static final int CONDITION = -2;
    //waitStatus值表明下一个acquireShared无条件传播
    static final int PROPAGATE = -3;
    /**
      * waitStatus,仅能为以上几个状态值或者为0
      * SIGNAL:该节点的后继节点处于足额色状态,在当前节点释放或者取消状态,要保证后继节点释放。
      * 为了防止循环,acquire方法必须首先声明它们需要信号,然后尝试原子获取,然后是在失败时阻塞。
      * CANCELLED:由于超时或者打断导致节点取消,节点永远离不开状态,在特殊情况下,具有取消节点的线程永远不会再次阻塞。
      * CONDITION:该节点是在一个条件队列,在转移之前,该节点不会被当作一个同步队列节点,在转移时,节点状态会被设置为0
      * PROPAGATE:应将releaseShared传播到其他节点。如果这个节点为头节点,设置doReleaseShared以确保传播继续,即使有其他操作自从介入。
      * 0
      */
    volatile int waitStatus;
    //当前节点的前驱节点
    volatile Node prev;
    //当前节点的后继节点
    volatile Node next;
    //
    volatile Thread thread;
    Node nextWaiter;
    final boolean isShared() {
        return nextWaiter == SHARED;
    }
    final Node predecessor() throws NullPointerException {
        Node p = prev;
        if (p == null)
            throw new NullPointerException();
        else
            return p;
    }
    Node() {    // Used to establish initial head or SHARED marker
    }
    
    Node(Thread thread, Node mode) {     // Used by addWaiter
        this.nextWaiter = mode;
        this.thread = thread;
    }
    
    Node(Thread thread, int waitStatus) { // Used by Condition
        this.waitStatus = waitStatus;
        this.thread = thread;
    }
}
```


