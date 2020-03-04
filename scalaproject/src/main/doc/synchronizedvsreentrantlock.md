# Synchronized vs ReentrantLock
## 两个都是可重入锁
两者都是可重入锁。“可重入锁”概念是：自己可以再次获取自己的内部锁。比如一个线程获得了某个对象的锁，此时这个对象锁还没有释放，
当其再次想要获取这个对象的锁的时候还是可以获取的，如果不可锁重入的话，就会造成死锁。同一个线程每次获取锁，锁的计数器都自增1，
所以要等到锁的计数器下降为0时才能释放锁。

##  synchronized 依赖于 JVM 而 ReentrantLock 依赖于 API
synchronized 是依赖于 JVM 实现的，前面我们也讲到了 虚拟机团队在 JDK1.6 为 synchronized 关键字进行了很多优化，
但是这些优化都是在虚拟机层面实现的，并没有直接暴露给我们。ReentrantLock 是 JDK 层面实现的（也就是 API 层面，
需要 lock() 和 unlock() 方法配合 try/finally 语句块来完成），所以我们可以通过查看它的源代码，来看它是如何实现的。

## ReentrantLock 比 synchronized 增加了一些高级功能

ReentrantLock提供了一种能够中断等待锁的线程的机制，通过lock.lockInterruptibly()来实现这个机制。
也就是说正在等待的线程可以选择放弃等待，改为处理其他事情。
ReentrantLock可以指定是公平锁还是非公平锁。而synchronized只能是非公平锁。所谓的公平锁就是先等待的线程先获得锁。 
ReentrantLock默认情况是非公平的，可以通过 ReentrantLock类的ReentrantLock(boolean fair)构造方法来制定是否是公平的。
synchronized关键字与wait()和notify()/notifyAll()方法相结合可以实现等待/通知机制，ReentrantLock类当然也可以实现，
但是需要借助于Condition接口与newCondition() 方法。Condition是JDK1.5之后才有的，它具有很好的灵活性，
比如可以实现多路通知功能也就是在一个Lock对象中可以创建多个Condition实例（即对象监视器），线程对象可以注册在指定的Condition中，
从而可以有选择性的进行线程通知，在调度线程上更加灵活。 在使用notify()/notifyAll()方法进行通知时，被通知的线程是由 JVM 选择的，
用ReentrantLock类结合Condition实例可以实现“选择性通知” ，这个功能非常重要，而且是Condition接口默认提供的。
而synchronized关键字就相当于整个Lock对象中只有一个Condition实例，所有的线程都注册在它一个身上。
如果执行notifyAll()方法的话就会通知所有处于等待状态的线程这样会造成很大的效率问题，而Condition实例的signalAll()方法 
只会唤醒注册在该Condition实例中的所有等待线程。


## synchronized 关键字和 volatile 关键字的区别
>* volatile关键字是线程同步的轻量级实现，所以volatile性能肯定比synchronized关键字要好。
>但是volatile关键字只能用于变量而synchronized关键字可以修饰方法以及代码块。synchronized关键字在JavaSE1.6之后进行了
>主要包括为了减少获得锁和释放锁带来的性能消耗而引入的偏向锁和轻量级锁以及其它各种优化之后执行效率有了显著提升，
>实际开发中使用 synchronized 关键字的场景还是更多一些。
>* 多线程访问volatile关键字不会发生阻塞，而synchronized关键字可能会发生阻塞
>* volatile关键字能保证数据的可见性，但不能保证数据的原子性。synchronized关键字两者都能保证。
>* volatile关键字主要用于解决变量在多个线程之间的可见性，而 synchronized关键字解决的是多个线程之间访问资源的同步性。


## ThreadLocal
ThreadLocal类主要解决的就是让每个线程绑定自己的值，可以将ThreadLocal类形象的比喻成存放数据的盒子，盒子中可以存储每个线程的私有数据。

如果你创建了一个ThreadLocal变量，那么访问这个变量的每个线程都会有这个变量的本地副本，这也是ThreadLocal变量名的由来。
他们可以使用 get（） 和 set（） 方法来获取默认值或将其值更改为当前线程所存的副本的值，从而避免了线程安全问题。

ThreadLocal内存泄漏,Key为Thread,Value是Object对象,key是弱引用,value是强引用,key删除之后，value不能删除，就会造成内存泄漏。
在删除key之后，执行remove方法,或者将value变成弱引用,在不需要的时候，发生gc，将资源回收。


## 线程池的好处
资源池的好处:降低资源消耗;提高响应速度;提高线程的可管理性。


## Callable于Runnable
Runnable 接口不会返回结果或抛出检查异常，但是**Callable 接口**可以。所以，
如果任务不需要返回结果或抛出异常推荐使用 Runnable 接口，这样代码看起来会更加简洁。


## 执行execute()方法和submit()方法的区别是什么呢？
>* execute()方法用于提交不需要返回值的任务，所以无法判断任务是否被线程池执行成功与否；
>* submit()方法用于提交需要返回值的任务。线程池会返回一个Future 类型的对象，通过这个 Future 对象可以判断任务是否执行成功，
>并且可以通过 Future 的 get()方法来获取返回值，get()方法会阻塞当前线程直到任务完成，而使用 get（long timeout，TimeUnit unit）
>方法则会阻塞当前线程一段时间后立即返回，这时候有可能任务没有执行完。


