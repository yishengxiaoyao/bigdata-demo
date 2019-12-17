# Zookeeper
在Zookeeper中，父节点中，有很多子节点，子节点中，序列号最小的获取到锁，当这个节点被删除后者释放锁，后面的节点才能获取到。

创建节点不知道是否成功？
使用child-<sessionid>来命名节点名字，来解决这个问题。
一个故障转移的客户端拥有相同的会话，通过在当前child节点中是否含有当前会话节点。
如果每一个客户端都对parent节点进行监视，如果引起集群效应，如果只监视在他之前的一个节点，这样就可以避免这样的情况。

WriteLock实现分布式锁，并考虑了部分故障和从众的影响。

LockListener实例使用异步回调模型，它在获取锁时，调用lockAcquired方法，在释放锁时，调用lockReleased方法。

## 参考文献
[Distributed Coordination With ZooKeeper Part 1: Introduction](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part)
[Distributed Coordination With ZooKeeper Part 2: Test Drive](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part1)
[Distributed Coordination With ZooKeeper Part 3: Group Membership Example](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part2)
[Distributed Coordination With ZooKeeper Part 4: Architecture from 30,000 Feet](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part3)
[Distributed Coordination With ZooKeeper Part 5: Building a Distributed Lock](https://nofluffjuststuff.com/blog/scott_leberknight/2013/07/distributed_coordination_with_zookeeper_part_5_building_a_distributed_lock)
[Distributed Coordination With ZooKeeper Part 5: Building a Distributed Lock](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part4)
[Distributed Coordination With ZooKeeper Part 6: Wrapping Up](http://www.sleberknight.com/blog/sleberkn/entry/distributed_coordination_with_zookeeper_part5)
[Distributed Coordination With ZooKeeper Part 5: Building a Distributed Lock](sleberknight.com/blog/sleberkn/entry/building_a_distributed_lock_revisited)
[twitter distribute lock](https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/zookeeper/DistributedLockImpl.java)
[Apache Curator: distributed (try) locks](https://simplydistributed.wordpress.com/2016/12/21/apache-curator-distributed-try-locks/)
