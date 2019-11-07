#进程和线程

##进程
在python中创建进程的方式有两种:使用os的fork方法；使用multiprocessing模块。

###使用os的fork方法创建进程
fork方法调用一次，返回两次。原因在于操作系统将当前进程(父进程)复制出一份进程(子进程)。子进程中永远返回0，父进程中返回的是子进程的ID。
```
#!/usr/bin/python
# -*- coding: UTF-8 -*-
import os
if __name__ == '__main__':
    print 'current process (%s) start .. '%(os.getpid())
    pid = os.fork()
    if pid < 0:
        print 'error in fork'
    elif pid == 0:
        print 'I am child process(%s) and my parent process is (%s)',(os.getpid(),os.getppid())
    else:
        print 'I(%s) created a child process (%s).',(os.getpid(),pid)
```
### 使用multiprocessing模块创建多进程
使用Process类来创建进程对象，在创建的过程中，只需要传入一个执行函数和函数的参数，用start()方法启动进程，用join()方法实现进程间的同步。
```
#!/usr/bin/python
# -*- coding: UTF-8 -*-
import  os
from multiprocessing import Process

def run_proc(name):
    print 'child process %s (%s) Running...' %(name,os.getpid())

if __name__ == '__main__':
    print 'parent process %s.' %os.getpid()
    for i in range(5):
        p=Process(target=run_proc,args=(str(i),))
        print 'process will start'
        p.start()
    p.join()
    print 'process end'
```
###使用multiprocessing的Pool类来创建进程
Pool对象调用join()方法会等待所有子进程进行完毕，调用join()之前必须先调用close(),调用close()之后就不能继续添加新的Process了。
```
import  os,time,random
from multiprocessing import Pool
def run_task(name):
    print 'Task %s (pid=%s) is running...'%(name,os.getpid())
    time.sleep(random.random()*3)
    print 'Task %s end.'%name

if __name__ == '__main__':
    print 'Current process %s.' %os.getpid()
    p = Pool(processes=3)
    for i in range(5):
        p.apply_async(run_task,args=(i,))
    print 'Waiting for allsubprocess done...'
    p.close()
    p.join()
    print 'All subprocess done.'
```
### 进程间通信
Queue与Pipe的区别在于Pipe常用来在于两个进程间通信，Queue用来在多个进程间实现通信。
### Queue方式
Queue是多进程实现的队列，可以使用Queue实现多进程之间的数据传递。

Put方法用以掺入数据到队列中，如果超时有抛出Queue.Full异常。如果blocked(默认为True)为False，但Queue已满，会立即抛出Queue.Full异常。

Get方法可以从队列读取并且删除一个元素。在等待的时间内没有数据，抛出Queue.Empty异常，如果blocked为False,分两种情况:如果Queue有一个值可用，
则立即返回该值;否则，如果队列为空，则立即抛出Queue.Empty异常。

```
import os,time,random
from multiprocessing import Process,Queue

def proc_write(q,urls):
    print 'Process %s is writing...' %(os.getpid())
    for url in urls:
        q.put(url)
        print ('Put %s to queue'%url)
        time.sleep(random.random())
def proc_read(q):
    print 'Process %s is reading...' % (os.getpid())
    while True:
        url = q.get(True)
        print ('Get %s from queue.'%url)

if __name__=='__main__':
    #父进程创建Queue，并传递给各个子进程
    q = Queue()
    proc_write_1 = Process(target=proc_write,args=(q,['url1','url2','url3']))
    proc_write_2 = Process(target=proc_write, args=(q, ['url4', 'url5', 'url6']))
    proc_read_1 = Process(target=proc_read,args=(q,))
    #启动子进程proc_write
    proc_write_1.start()
    proc_write_2.start()
    #启动读
    proc_read_1.start()
    #等待write结束
    proc_write_1.join()
    proc_write_2.join()
    #proc_read进程进入死循环，只能强行终止
    proc_read_1.terminate()
```
### Pipe
Pipe常用来在两个进程间进行通信，两个进程分别位于管道的两端。
Pipe方法返回(conn1,conn2)代表一个管道的两端。Pipe有一个duplex参数,如果duplex参数为true,管道为全双工模式。
如果duplex为False,conn1只负责接收消息，conn2只负责发送消息。如果没有消息可接受，recv方法会一直阻塞。
如果管道被关闭，recv方法会抛出EOFError。
```
import os,time,random
import multiprocessing

def proc_send(pipe,urls):
    for url in urls:
        print 'Process %s send:%s '%(os.getpid(),url)
        pipe.send(url)
        time.sleep(random.random())

def proc_recv(pipe):
    while True:
        print 'Process %s recv"%s' % (os.getpid(),pipe.recv())
        time.sleep(random.random())


if __name__ == '__main__':
    pipe = multiprocessing.Pipe()
    p1 = multiprocessing.Process(target=proc_send,args=(pipe[0],['url_'+str(i) for i in range(10) ]))
    p2 = multiprocessing.Process(target=proc_recv,args=(pipe[1],))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
```

## 多线程

### 用threading创建多线程
threading创建线程的方式有:把一个函数传入并创建Thread实例，然后调用start方法执行;
直接从threading.Thread继承并创建线程类，然后重写__init__方法和run方法。
```
import random,time
import threading

#新线程的代码
def thread_run(urls):
    print 'Current %s is running...' %threading.current_thread().name
    for url in urls:
        print '%s--->%s' %(threading.current_thread().name,url)
        time.sleep(random.random())
    print '%s end.' %threading.current_thread().name
print '%s is running...' %threading.current_thread().name
t1=threading.Thread(target=thread_run,args=(['url1','url2','url3'],))
t2=threading.Thread(target=thread_run,args=(['url4','url5','url6'],))
t1.start()
t2.start()
t1.join()
t2.join()
print '%s ended.' %threading.current_thread().name
```
第二种方式
```
import random,time
import threading

class myThread(threading.Thread):
    def __init__(self,name,urls):
        threading.Thread.__init__(self,name=name)
        self.urls = urls
    #新线程的代码
    def run(self):
        print 'Current %s is running...' %threading.current_thread().name
        for url in self.urls:
            print '%s--->%s' %(threading.current_thread().name,url)
            time.sleep(random.random())
        print '%s end.' %threading.current_thread().name
print '%s is running...' %threading.current_thread().name
t1=myThread(name='Thread_1',urls=['url1','url2','url3'])
t2=myThread(name='Thread_2',urls=['url4','url5','url6'])
t1.start()
t2.start()
t1.join()
t2.join()
print '%s ended.' %threading.current_thread().name
```
###线程同步
使用Thread对象的Lock和RLock可以实现简单的线程同步，这两个对象都有acquire和release方法。
```
import random,time
import threading

mylock = threading.RLock()
num = 0
class myThread(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self,name=name)
    #新线程的代码
    def run(self):
        global num
        while True:
            mylock.acquire()
            print '%s locked,Number: %d' %(threading.current_thread().name,num)
            if num >= 4:
                mylock.release()
                print '%s released,Number: %d' % (threading.current_thread().name, num)
                break
            num += 1
            print '%s released,Number: %d' % (threading.current_thread().name, num)
            mylock.release()

if __name__ == '__main__':
    thread1 = myThread('Thread_1')
    thread2 = myThread('Thread_2')
    thread1.start()
    thread2.start()
```

##分布式进程

