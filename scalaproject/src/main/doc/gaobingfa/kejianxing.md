# Java并发编程实战

## 可见性、原子性和有序性

为了合理利用CPU的高性能,平衡CPU、内存、IO的速度差异,计算机体系机构、操作系统、编译程序都做出了贡献,主要体现为:
>* CPU增加了缓存,以均衡与内存的速度差异;
>* 操作系统增加了进程、线程,已分时复用CPU,进而均衡CPU与IO设备的速度差异;
>* 编译程序优化指令执行次序,使得缓存能够得到更加合理利用。

一个线程对共享变量的修改,另一个线程能够立刻看到,成为可见性。

把一个或者多个操作在CPU执行的过程中不被中断的特性成为原子性。

解决原子性的问题,是要保证中间状态对外不可见。

## Java内存模型如何解决可见性和有序性问题

### happen-before原则
前面一个操作的结果对后续操作是可见的。

>* 程序的顺序性原则。
>* volatile变量原则。
>* 传递性。
>* 管程中锁的原则。
>* 线程start原则。
>* 线程join原则。

```
class Account{
    private int balance;
    void transfer(Account transfer,int amt){
        synchronized(Account.class){
            if(this.balance > amt){
                this.balance -= amt;
                target.balance = amt;
            }
        }
    }
}
```


## 查找问题
top -c 查找那个进程占用的资源CPU多
top -Hp 查找那个线程出现的问题
jstack 查看线程的栈信息
