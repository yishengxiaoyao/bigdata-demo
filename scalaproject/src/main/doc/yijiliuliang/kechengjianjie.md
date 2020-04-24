# 亿级流量课程

## Redis持久化
Redis持久化的机制有两种:RDB、AOF。

| |含义|优点|缺点|
|----|----|----|----|
|RDB|对Redis中的数据执行周期性的持久化(每次生成一个快照,二进制文件)|1.适合做冷备;2.性能影响小;3.数据恢复快;|1.故障时,丢失数据多;2.数量大性能低|
|AOF|每条写入命令作为日志,写入AOF文件中,以追加方式添加数据(文本)|1.故障时,丢失数据少;2.AOF写入性能高;3.rewrite操作对redis主线程影响小;4.AOF文件内容比较容易理解|1.日记文件稍大;2.性能低;3.数据恢复慢|

AOF添加数据的策略:
>* always：服务器在每执行一个事件就把AOF缓冲区的内容强制性的写入硬盘上的AOF文件里，保证了数据持久化的完整性，效率是最慢的但最安全的； 
>* everysec：服务端每隔一秒才会进行一次文件同步把内存缓冲区里的AOF缓存数据真正写入AOF文件里，兼顾了效率和完整性，极端情况服务器宕机只会丢失一秒内对Redis数据库的写操作；
>* no：表示默认系统的缓存区写入磁盘的机制，不做程序强制，数据安全性和完整性差一些。

## 数据恢复
>* 如果只配置 AOF ，重启时加载 AOF 文件恢复数据；
>* 如果同时配置了 RDB 和 AOF ，启动只加载 AOF 文件恢复数据；
>* 如果只配置 RDB，启动将加载 dump 文件恢复数据。

## 删除key的策略
>* 定时删除：在设置键的过期时间的同时，创建定时器，让定时器在键过期时间到来时，即刻执行键值对的删除；
>* 定期删除：每隔特定的时间对数据库进行一次扫描，检测并删除其中的过期键值对；
>* 惰性删除：键值对过期暂时不进行删除，至于删除的时机与键值对的使用有关，当获取键时先查看其是否过期，过期就删除，否则就保留；

删除key的机制
>* noeviction: 当内存不足以容纳新写入数据时，新写入操作会报错；
>* allkeys-lru：当内存不足以容纳新写入数据时，在键空间中移除最近最少使用的 key；
>* allkeys-random：当内存不足以容纳新写入数据时，在键空间中随机移除某个 key；
>* volatile-lru：当内存不足以容纳新写入数据时，在设置了过期时间的键空间中，移除最近最少使用的 key；
>* volatile-random：当内存不足以容纳新写入数据时，在设置了过期时间的键空间中，随机移除某个 key；
>* volatile-ttl：当内存不足以容纳新写入数据时，在设置了过期时间的键空间中，有更早过期时间的 key 优先移除；


## Redis Replication

### 复制的过程
1.slave node启动,仅仅保存master node的信息:redis.conf中存储了master node的配置(ip+端口)

2.slave node 定时检查是否需要与master连接:内部有个定时任务,每秒检查是否有新的master node要连接和复制,如果发现,就跟master node建立socket网络连接。

3.slave node 发送 ping 命令给 master node。

4.口令认证:如果 master 设置了requirepass,那么 salve node 必须发送 masterauth的口令过去进行认证。

5.master node 第一次执行全量复制，将所有数据发给 slave node

6.master node 后续持续将写命令，异步复制给 slave node

### 数据同步相关的核心机制
>* master 和 slave 都会维护一个 offset:master 会在自身不断累加 offset，slave 也会在自身不断累加 offset;slave 每秒都会上报自己的 offset 给 master，同时 master 也会保存每个 slave的 offset;这个倒不是说特定就用在全量复制的，主要是 master 和 slave 都要知道各自的数据的 offset，才能知道互相之间的数据不一致的情况
>* backlog:master node 有一个 backlog，默认是 1MB 大小;master node 给 slave node 复制数据时，也会将数据在 backlog 中同步写一份;backlog 主要是用来做全量复制中断候的增量复制的;master_replid和offset存储在RDB文件中。当从实例在复制过程中，因网络闪断等原因造成的数据丢失场景，Redis能够从rdb文件中重新加载master_replid和offset，从而使部分重新同步成为可能。因为补发的数据远小于全量数据，所以可以有效的避免全量复制带来的负载和消耗。之前说过，从节点连接主节点之后，会使用master_replid和master_repl_offset请求主节点，首先判断master_replid是否和自己的master_replid一致，然后检查请求中的master_repl_offset是否能从缓冲区（replication backlog）中获取，如果偏移量在backlog范围内，那么可以进行部分复制。如果在断开连接期间主节点收到的写入命令的数量超过了backlog缓冲区的容量，那么会进行全量复制。默认情况下backlog为1MB。
>* master run id:通过 info server 命令可以看到 master run id;因为使用host+ip来定位master是不靠谱的，如果master node重启或者数据出现了变化，那么slave应该根据不同的master run id进行区分，run id不同就需要做一次全量复制。如果需要不更改 run id 重启 redis，可以使用 redis-cli debug reload 命令;
>* psync:从节点使用psync从master node进行复制,psync runid offset;master node会根据自身的情况返回响应信息,可能是FULLRESYNC runid offset触发全量复制,可能是CONTINUE触发增量复制。

### 全量复制
>* master 执行 bgsave，在本地生成一份 rdb 快照文件
>* master node 将 rdb 快照文件发送给 salve node:如果 rdb 复制时间超过 60 秒（可通过 repl-timeout 属性配置），那么 slave node 就会认为复制失败，可以适当调节大这个参数,对于千兆网卡的机器，一般每秒传输 100MB，6G 文件，很可能超过 60s
>* master node 在生成 rdb 时，会将所有新的写命令缓存在内存中，在 salve node 保存了 rdb 之后，再将新的写命令复制给 salve node，保证主从数据一致
>* client-output-buffer-limit slave 256MB 64MB 60: 如果在复制期间，内存缓冲区持续消耗超过 64MB，或者一次性超过 256MB，那么停止复制，复制失败,意思就是在等待 slave 同步 rdb 文件的时候，master 接收写的命令在缓冲区超过了 64m 的数据，那么此次复制失败
>* slave node 接收到 rdb 之后，清空自己的旧数据，然后重新加载 rdb 到自己的内存中，同时基于旧的数据版本对外提供服务,如果 slave node 开启了 AOF，那么会立即执行 BGREWRITEAOF，重写 AOF

###增量复制
>* 如果全量复制过程中，master-slave 网络连接断掉，那么 salve 重新连接 master 时，会触发增量复制
>* master 直接从自己的 backlog 中获取部分丢失的数据，发送给 slave node，默认 backlog 就是 1MB
>* master 就是根据 slave 发送的 psync 中的 offset 来从 backlog 中获取数据的

### 断点续传
从 redis 2.8 开始，就支持主从复制的断点续传，如果主从复制过程中，网络连接断掉了，那么可以接着上次复制的地方，继续复制下去，而不是从头开始复制一份master node 会在内存中创建一个 backlog，master 和 slave 都会保存一个 replica offset 和 master id，offset 就是保存在 backlog 中的。如果 master 和 slave 网络连接断掉了，slave 会让 master 从上次的 replica offset 开始继续复制，但是如果没有找到对应的 offset，那么就会执行一次 resynchronization。

### key过期
slave 不会过期 key，只会等待 master 过期 key。master过期key之后，给slave发送del命令。

### heartbeat
主从节点互相都会发送 heartbeat 信息:master 默认每隔 10 秒发送一次 heartbeat;salve node 每隔 1 秒发送一个 heartbeat。
### 异步复制
master 每次接收到写命令之后，先在内部写入数据，然后异步发送给 slave node

## 哨兵
### 哨兵的功能
>* 集群监控：负责监控 redis master 和 slave 进程是否正常工作
>* 消息通知：如果某个 redis 实例有故障，那么哨兵负责发送消息作为报警通知给管理员
>* 故障转移：如果 master node 挂掉了，会自动转移到 slave node 上
>* 配置中心：如果故障转移发生了，通知 client 客户端新的 master 地址

哨兵结构的解释:
>* 哨兵至少需要 3 个实例，来保证自己的健壮性
>* 哨兵 + redis 主从的部署架构，是不会保证数据零丢失的，只能保证 redis 集群的高可用性
>* 对于哨兵 + redis 主从这种复杂的部署架构，尽量在测试环境和生产环境，都进行充足的测试和演练

### 哨兵主备切换的数据丢失问题
异步复制:部分数据还没复制到 slave，master 就宕机了，此时这些部分数据就丢失了。
集群脑裂:一个集群中的 master 恰好网络故障，导致与 sentinal 联系不上了，sentinal 把另一个 slave 提升为了 master。此时就存在两个 master了。停止掉其中的一个 master，手动切换成 slave，当它连接到提升后的 master 的时候，会开始同步数据，那么自己脑裂期间接收的写数据就被丢失了。

解决异步复制和脑裂导致的数据丢失:
>* 配置:min-slaves-to-write 1;min-slaves-max-lag 10;
>* 求至少有 1 个 slave，数据复制和同步的延迟不能超过 10 秒，如果超过 1 个 slave，数据复制和同步的延迟都超过了 10 秒钟，那么这个时候，master 就不会再接收任何请求了。



## 参考文献
[3w字深度好文|Redis面试全攻略，读完这个就可以和面试官大战几个回合了](https://juejin.im/post/5e520c0b6fb9a07ca5303bf5?utm_source=gold_browser_extension)
[图解分析 Redis 的 RDB 和 AOF 两种持久化机制的工作原理](https://zq99299.github.io/note-book/cache-pdp/redis/009.html)
[Redis 的 RDB 和 AOF 两种持久化机制的优劣势对比](https://zq99299.github.io/note-book/cache-pdp/redis/010.html)
[在项目中部署 redis 企业级数据备份方案以及各种踩坑的数据恢复容灾演练](https://zq99299.github.io/note-book/cache-pdp/redis/013.html)
[Redis Replication 的完整流运行程和原理的再次深入剖析](https://zq99299.github.io/note-book/cache-pdp/redis/017.html)

## 亿级流量系统缓存架构需要的内容

### 如何让 redis 集群支撑几十万 QPS 高并发 + 99.99% 高可用 + TB 级海量数据 + 企业级数据备份与恢复？
### 如何支撑高性能以及高并发到极致？同时给缓存架构最后的安全保护层？
(nginx + lua) + redis + ehcache 的三级缓存架构

### 高并发场景下，如何解决数据库与缓存双写的时候数据不一致的情况？

企业级的完美的数据库 + 缓存双写一致性解决方案

### 如何解决大 value 缓存的全量更新效率低下问题？

缓存维度化拆分解决方案

### 如何将缓存命中率提升到极致？

双层 nginx 部署架构，以及 lua 脚本实现的一致性 hash 流量分发策略

###如何解决高并发场景下，缓存重建时的分布式并发重建的冲突问题？

基于 zookeeper 分布式锁的缓存并发重建解决方案

### 如何解决高并发场景下，缓存冷启动 MySQL 瞬间被打死的问题？

基于 storm 实时统计热数据的分布式快速缓存预热解决方案

### 如何解决热点缓存导致单机器负载瞬间超高？

基于 storm 的实时热点发现，以及毫秒级的实时热点缓存负载均衡降级

### 如何解决分布式系统中的服务高可用问题？避免多层服务依赖因为少量故障导致系统崩溃？

基于 hystrix 的高可用缓存服务，资源隔离 + 限流 + 降级 + 熔断 + 超时控制

### 如何应用分布式系统中的高可用服务的高阶技术？

基于 hystrix 的容错 + 多级降级 + 手动降级 + 生产环境参数优化经验 + 可视化运维与监控

### 如何解决恐怖的缓存雪崩问题？避免给公司带来巨大的经济损失？

独家的事前 + 事中 + 事后三层次完美解决方案

### 如何解决高并发场景下的缓存穿透问题？避免给 MySQL 带来过大的压力？

缓存穿透解决方案

### 如何解决高并发场景下的缓存失效问题？避免给 redis 集群带来过大的压力？

缓存失效解决方案



## 亿级流量多级缓存

### 时效性要求非常高的数据：库存

### 时效性要求不高的数据：商品的基本信息



