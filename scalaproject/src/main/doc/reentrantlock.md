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
    //通过互斥模式获取,忽略终端
   if (!tryAcquire(arg) &&
        acquireQueued(addWaiter(Node.EXCLUSIVE), arg))
        selfInterrupt();
}
//ReentrantLock.FairSync
protected final boolean tryAcquire(int acquires) {
        //获取当前线程
        final Thread current = Thread.currentThread();
        //获取状态机的值,从AbstractQueuedSynchonizer的变量
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
    Node t = tail; // Read fields in reverse initialization order
    Node h = head;
    Node s;
    return h != t &&
        ((s = h.next) == null || s.thread != Thread.currentThread());
}
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
```

### 多线程

#### 加锁过程

#### 解锁过程




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