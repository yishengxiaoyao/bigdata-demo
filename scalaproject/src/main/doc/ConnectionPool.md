# Connection Pool

## HikariCP
HikariCP的getConnection获取的数量比其他JDBC获取的连接数据要少。在委托方面作了优化。
## HikariCP为什么这么快
> * 字节码级别优化(很多方法通过JavaAsist生成):扁平化继承关系、浅复制成员变量、消除强制转换。
> * 微小优化：使用FastList<Statement>代替ArrayList<Statement>(FastList替代ArrayList,去掉了范围检查和从尾部到头部删除元素);
引入无锁集合CurrentBag(在无锁设计中提供了ThreadLocal缓存和队列窃取，提供高并发性并最小化错误共享的发生);代理类的优化(去掉getstatic调度用,
invokestatic在JVM中的优化更简单，stack size变小了)。

关于优化的详细内容，请求参考相应文章:<br>
[Down-the-Rabbit-Hole](https://github.com/brettwooldridge/HikariCP/wiki/Down-the-Rabbit-Hole)
[DB连接池HikariCP为什么如此快](https://mp.weixin.qq.com/s/JYhfFSK11YUBlr_ika_y_w)
## Druid

Druid是阿里巴巴开源的数据库链接池项目，该项目拥有强大的监控功能，能放SQL注入，内置的Logging能诊断Hack应用行为。
