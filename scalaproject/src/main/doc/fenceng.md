# 高并发
## 高并发系统
应对高并发的方法:
>* Scale-out(横向扩展):分而治之是一种常见的高并发系统设计方法,采用分布式部署的方式把流量分流开,让每个服务器都承担一部分并发和流量。
>* 缓存:使用缓存来提高系统的性能。
>* 异步:未处理完成之前我们可以让请求先返回,在数据转背好之后再通知请求方,这样可以在单位时间内处理更多的请求。

如何选择scale-out与scale-up?
使用堆砌硬件解决的问题就用硬件来解决,使用scale-out方式,如果系统并发超过单机的极限时,就用scale-out.

为什么缓存可以大幅度提升系统的性能?
数据一般都是持久化到磁盘上。普通磁盘数据是由机械手臂、刺头、转轴、磁盘组成，磁盘有分为磁道、柱面和扇区。
扇区是存储介质，每个磁盘被划分为多个同心圆,信息存储在同心圆之中,这些同心圆就是磁道。磁头寻找信息话费的时间叫做寻道时间。
普通磁盘的寻道时间是10ms左右，在整个计算机体系中磁盘是最慢的一环,使用内存作为存储介质的缓存,以提升性能。

## 为什么要分层
分层的设计的好处:
>* 简化系统设计，让不同的人专注做某一层次的事情。
>* 分层之后可以做到很高的复用。
>* 最后一次,分层架构可以让我们更容易做横向扩展。

分层架构中每一层的作用:
>* 终端显示层:各端模块渲染并执行显示的层。当权主要是velocity渲染、js渲染、jsp渲染、移动端显示等。
>* 开放接口层:将service层方法封装成开放接口,同时进行网关安全控制和流量控制等。
>* web层:主要对访问控制进行转发,各类基本参数校验,或者不复用的业务简单处理等。
>* service层:业务逻辑层
>* Manager层:通用业务护理层。
>* DAO层:数据访问层。

分层架构的不足:
>* 在接收到请求是可以直接访问DB获取结果,却偏偏要在中间插入多个层次,并且有可能每个层次只是简单地做数据的传递;小需求开发,成本高。
>* 每个层次独立部署,层次间通过网络交互,多层次的架构在性能上有消耗。

## 系统设计目标
### 如何设计系统性能
高并发系统设计的三大目标:高性能、高可用、可扩展。
高并发是值运用设计手段让系统能够处理更多的用护并发请求，也就是承担更大的流量。

性能优化原则:
>* 性能优化一定不能盲目,一定是问题导向的。
>* 性能优化也遵循八二原则。
>* 性能优化也要有数据支撑。
>* 性能优化的过程是持续的。

性能的度量指标:
>* 平均值:把这段时间所有的请求相应时间数据相加,在除以总请求数。　
>* 最大值:这段时间内所有请求响应时间最长的值。
>* 分位值:落在响应时间的值占用的比重。

高并发下的性能优化:
>* 提高系统的处理核心数(吞吐量=核心数(并发进程数)/响应时间)
>* 减少单次任务线响应时间(根据类型不同选择不同的处理方法)
>* 		调用非阻塞的rpc调用
>* 		将计算密集型和和IO密集型的逻辑分割开,单独线程池,调整线程比例压榨单机性能
>* 		使用缓存,减少读取的时间
>* 		采用享元模式,用好对象池和本地线程空间,尽量减少对象黄建与销毁的开销,提高复用。
>* 		业务拆分:可以借助中间件做一些处理。
>* 		fork/join:对线程处理
>* 		数据库撇值优化，查询优化。
### 系统如何做到高可用