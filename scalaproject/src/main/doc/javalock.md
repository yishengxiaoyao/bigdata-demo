# Java 锁体系

## 锁的分类
线程是否需要锁住共享资源:
>* 需要锁住:悲观锁 
>* 不需要锁住:乐观锁(使用CAS算法实现)

多个线程竞争同步资源的流程与细节做区分:
>* 无锁:不锁住资源,多个线程只有一个能修改资源成功，其他会线程会重试
>* 偏向锁:同一个线程执行同步资源时,自动获取资源
>* 轻量级锁:多个线程竞争同步资源时,没有获取资源的线程会自旋等待锁释放。
>* 重量锁:多个线程竞争同步资源时,没有获取资源的线程会阻塞等待唤醒。

多个线程竞争锁是是否需要排队:
>* 需要排队:公平锁
>* 不需要排队:先尝试插队,如果插队失败在排队,是非公平锁。

一个线程的多个流程能不能获取同一个锁:
>* 可以:可重入锁
>* 不可以:非可重入锁

多个线程能不能共享同一把锁:
>* 能:共享
>* 不能:排他锁


## CAS算法
CAS(Compare And Swap)。
CAS是一种无锁算法:基于硬件原语实现,在不使用锁(没有线程被阻塞)的情况下实现多线程之间的变量同步。
JDK中锁的实现:java.util.concurrent包中的原子类(AtomicInteger)就是通过CAS来实现了的乐观锁。

CAS算法会涉及三个参数:需要读写的内存值V、进行比较的值A、要写入的新值B。当且仅当预期值A和内存值V相同时,将内存值V修改为B,否则什么也不做。

CAS算法会有如下问题:
>* ABA问题:一个线程将内存值A改为B,另一个线程将内存值B改为A。(解决方法:AtomicStampedReference在变量之前添加版本号,每次变更增加版本号,在比较的时候,也要比较版本号)。
>* 循环时间长开销大：CAS算法需要不断地自旋来读取最新的内存值，长时间读取不到就会造成不必要的CPU开销(自旋锁，不断重试)。
>* 只能保证一个共享变量的原子操作:CAS 只对单个共享变量有效，当操作涉及跨多个共享变量时 CAS 无效(AtomicReference类来保证引用对象之间的原子性,可以把多个变量放在一个对象中进行CSA操作)。

使用CAS可以保证原子操作,但是volatile不能。

如果你的字段是volatile，Java内存模型将在写操作后插入一个写屏障指令，在读操作前插入一个读屏障指令。这意味着如果你对一个volatile字段进行写操作，你必须知道：1、一旦你完成写入，任何访问这个字段的线程将会得到最新的值。2、在你写入前，会保证所有之前发生的事已经发生，并且任何更新过的数据值也是可见的，因为内存屏障会把之前的写入值都刷新到缓存。从Load到store到内存屏障，一共4步，其中最后一步jvm让这个最新的变量的值在所有线程可见，也就是最后一步让所有的CPU内核都获得了最新的值，但中间的几步（从Load到Store）是不安全的，中间如果其他的CPU修改了值将会丢失。

CAS与synchronized应用场景的比较:
简单的来说CAS适用于写比较少的情况下（多读场景，冲突一般较少），synchronized适用于写比较多的情况下（多写场景，冲突一般较多）
对于资源竞争较少（线程冲突较轻）的情况，使用synchronized同步锁进行线程阻塞和唤醒切换以及用户态内核态间的切换操作额外浪费消耗cpu资源；而CAS基于硬件实现，不需要进入内核，不需要切换线程，操作自旋几率较少，因此可以获得更高的性能。
对于资源竞争严重（线程冲突严重）的情况，CAS自旋的概率会比较大，从而浪费更多的CPU资源，效率低于synchronized。

## 悲观锁
悲观锁与乐观锁是一种广义概念,体现的是看线程同步的不同角度。

悲观锁认为自己在使用数据的时候一定有别的线程来修改数据,在获取数据的时候先加锁,确保数据不会被其他线程修改。

悲观锁的实现:关键字synchronized、Lock接口的实现类

悲观锁的适用场景:写操作比较多,先加锁可以保证写的数据时数据正确。

悲观锁的实现过程:多个线程尝试获取同步资源的锁(对同步资源加锁)-->某个线程对同步资源加锁成功并执行操作其他线程进入等待操作-->获取锁的线程操作完成之后会释放锁,然后CPU唤醒等待的其线程再尝试获取锁-->其他线程获取锁,在执行自己的操作。

## 乐观锁

乐观锁认为自己在使用数据时不会有其他的线程来修改数据，所以不用添加锁，只是在更新数据的时候去判断之前有没有其他线程更新这个数据。
乐观锁的实现:CAS算法、例如AtomicInteger类的原子自增就是通过CAS自旋实现。

乐观锁的适用场景:读操作比较多，不加锁的特点能够使其读操作的性能大幅度提升。

乐观锁的执行过程:线程直接获取同步资源数据执行各自的操作-->更新内存中的同步资源之前先判断资源是否被其他线程修改-->进行不同的操作。 


## 自旋锁
自旋锁（spinlock）：是指当一个线程在获取锁的时候，如果锁已经被其它线程获取，那么该线程将循环等待，然后不断的判断锁是否能够被成功获取，直到获取到锁才会退出循环。

自旋锁存在的问题:
>* 如果某个线程持有锁的时间过长，就会导致其它等待获取锁的线程进入循环等待，消耗CPU。使用不当会造成CPU使用率极高。
>* 上面Java实现的自旋锁不是公平的，即无法满足等待时间最长的线程优先获取锁。不公平的锁就会存在“线程饥饿”问题。
自旋锁的优点
自旋锁不会使线程状态发生切换，一直处于用户态，即线程一直都是active的；不会使线程进入阻塞状态，减少了不必要的上下文切换，执行速度快
非自旋锁在获取不到锁的时候会进入阻塞状态，从而进入内核态，当获取到锁的时候需要从内核态恢复，需要线程上下文切换。 （线程被阻塞后便进入内核（Linux）调度状态，这个会导致系统在用户态与内核态之间来回切换，严重影响锁的性能）

## 条件队列
Condition是一个多线程间协调通信的工具类,使得某个或者某些线程一起等待某个条件，只有当该条件具备时,这些等待线程才会被唤醒,从而重新获取锁。


## 读写锁
写锁(独占锁,排他锁),是指该锁只能被一个线程所持有。如果线程T对资源A添加了排他锁，其他线程不能再对A资源加任何类型的锁。获得写锁的线程既能读取数据又能修改数据。

读锁(共享锁)是指该锁可被多个线程锁持有。如果线程T对数据A添加共享锁后,其他线程只能对A资源添加共享锁,不能加排他锁,获得读锁的线程只能读数据,不能修改数据。
AQS中的state字段(int类型,32位),此处state上分别描述读锁和写锁的数量于是将state变量按位切割切分成两部分,读锁和写锁各占16位。


## 实现同步
### 自旋方式实现同步
```
volatile int status=0;//标识---是否有线程在同步块-----是否有线程上锁成功
void lock(){
	while(!compareAndSet(0,1)){
	}
	//lock
}
void unlock(){
	status=0;
}
boolean compareAndSet(int except,int newValue){
	//cas操作,修改status成功则返回true
}
```
缺点：耗费cpu资源。没有竞争到锁的线程会一直占用cpu资源进行cas操作，假如一个线程获得锁后要花费Ns处理业务逻辑，那另外一个线程就会白白的花费Ns的cpu资源
解决思路：让得不到锁的线程让出CPU

### yield + 自选锁实现同步
```
volatile int status=0;
void lock(){
	while(!compareAndSet(0,1)){
     yield();//自己实现
	}
	//lock

}
void unlock(){
	status=0;
}
```
要解决自旋锁的性能问题必须让竞争锁失败的线程不空转,而是在获取不到锁的时候能把cpu资源给让出来，yield()方法就能让出cpu资源，当线程竞争锁失败时，会调用yield方法让出cpu。
自旋+yield的方式并没有完全解决问题，当系统只有两个线程竞争锁时，yield是有效的。需要注意的是该方法只是当前让出cpu，有可能操作系统下次还是选择运行该线程，如果线程数比较多,
效率就会比较低。

### sleep+自旋方式实现同步
```
volatile int status=0;
void lock(){
	while(!compareAndSet(0,1)){
		sleep(10);
	}
	//lock

}
void unlock(){
	status=0;
}
```
缺点:不能很好确定sleep时间。

### park+自旋方式实现同步
```
volatile int status=0;
Queue parkQueue;//集合 数组  list

void lock(){
	while(!compareAndSet(0,1)){
		//
		park();
	}
	//lock    10分钟
   。。。。。。
   unlock()
}

void unlock(){
	lock_notify();
}

void park(){
	//将当期线程加入到等待队列
	parkQueue.add(currentThread);
	//将当期线程释放cpu  阻塞
	releaseCpu();
}
void lock_notify(){
	//得到要唤醒的线程头部线程
	Thread t=parkQueue.header();
	//唤醒等待线程
	unpark(t);
}
```
使用CAS、LockSupport、自选锁来实现的同步比较好，如果碰到多个线程,可以使用同步队列来判断需要对那个线程进行park、unpark操作。



## Node类
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


## CLH队列
CLH是一种基于双向链表数据库结构的队列,Java中的CLH队列是原CLH队列的一个变种,线程有原自旋机制改为阻塞机制,先进先出等待队列。




##参考文献
[CAS算法](https://juejin.im/post/5d6e69ec51882554575020b0)
[为什么volatile不能保证原子性而Atomic可以？](https://www.cnblogs.com/Mainz/p/3556430.html)
[乐观锁与悲观锁](http://mp.weixin.qq.com/s?__biz=Mzg2OTA0Njk0OA==&mid=2247484911&amp;idx=1&amp;sn=1d53616437f50b353e33edad6fda2e4f&source=41#wechat_redirect)
[JUC AQS ReentrantLock源码分析（一）](https://blog.csdn.net/java_lyvee/article/details/98966684)