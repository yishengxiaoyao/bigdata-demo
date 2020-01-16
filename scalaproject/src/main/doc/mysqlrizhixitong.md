# MySQL日志系统:一条SQL更新语句是如何执行的
日志系统: redo log(重做日志)和binlog(归档日志)。

## 重要的日志模块:redo log
MySQL WAL(Write-Ahead-Logging)关键点是先写日志,再写磁盘。
当更新数据时,InnoDB引擎就会先把记录写到redo log里面,并更新内存,这个是否更新就算完成,在合适的时候,将记录写入到磁盘里面。
redo log的标记:write pos 是当前记录的位置,一边写一边后羿;checkpoint是当前要擦出的位置,也是往后移动推移并且循环的,
擦除记录前要把记录更新到数据文件。
有了redo log,InnoDB就可以保证即使数据库发生异常重启,之前提交的记录都不会丢失,这个能力成为crash-safe。
## 重要的日志模块:binlog
Server层有自己的日志,称为binlog。redo log是InnoDB引擎特有的日志。

刚开始时,MySQL自带的引擎是MyISAM,但是MyISAM没有crash-safe的能力,binlog日志只能用于归档。

redo log与binlog的区别:
>* redo log是InnoDB引擎特有的;binlog是MySQL Server层实现的,所有引擎都可以使用。
>* redo log是物理日志,记录的是在某个数据页上做了什么修改;binlog是逻辑日志,记录的是这个语句的原始逻辑。
>* redo log是循环写的,空间固定会用完;binlog是可以追加写入的。追加写入是指binlog文件写到一定大小后会切换到下一个,并不会覆盖以前的日志。

更新流程:
>* 执行器先找引擎得到修改的记录,如果在内存中,直接返回;如果没有在内存中,从磁盘读取内存,然后在返回。
>* 执行器拿到数据后,修改之后,然后通过引擎将数据写入内存。
>* 引擎将数据更新到内存中,同时将这个更新操作记录到redo log里面,此时redo log处于prepare状态,然后告知执行器执行完成了,随时可以提交事务。
>* 执行器生成这个操作的binlog,并把binlog写入磁盘。
>* 执行器调用引擎的提交事务接口,引擎把刚刚写入的redo log改成commit状态,更新完成。


## 两阶段
binlog会记录所有的逻辑操作,并且是采用"追加写"的形式。

由于redo log和binlog是两个独立的逻辑,如果不用两阶段,就会出现问题
>* 先写redo log后写binlog:redo log写完,binlog没有写完,MySQL进程异常重启。系统崩溃之后,恢复的数据为更新后的数据,binlog恢复的数据是原先的数据。
>* 先写binlog 后写redo:binglog写完之后crash,事务无效,值没有变化,由于redo log还没写,通过binlog恢复的数据就多一个事务出来,与原库不同。

redo log用于保证crash-safe能力。innodb_flush_log_at_trx_commit这个参数设置为1的时候,表示每次事务的redo log都直接持久化到磁盘。
这样可以保证MySQL异常重启之后数据不丢失。

sync_binlog这个参数设置为1时,表示每次事务的binlog都持久化到磁盘。这样保证MySQL异常重启之后binlog不丢失。

binlog有两种模式,statement格式的记sql语句,row格式会记录行的内容,记两条,更新前和更新后都有。

redo只是完成了prepare,binlog失败,事务本身就会回滚,所以这个库里面status的值是0。

