# 深入理解StatefulSet
StatefulSet抽象为两种情况:
>* 拓扑状态:有先后的执行顺序。
>* 存储状态:读取的数据一致。

StatefulSet的核型功能：通过某种记录方式记录这些状态，然后在Pod被重新创建时，能够为新Pod恢复这些状态。

Service如何被访问:
>* 以Service的VIP(虚拟IP)方式:转发到该Service代理的某一个Pod上。
>* 以Service的DNS方式:通过DNS直接访问该Service代理的某一个Pod上(通过DNS解析到一个VIP，处理流程和VIP的处理方式一致(Normal Service);通过DNS解析到一个Pod的IP，直连,也叫Headless Service)。

Normal Service于Headless Service的区别:Headless Service不需要分配一个VIP，而是可以直接以DNS记录的方式解析出被代理Pod的IP地址。
## 拓扑状态

StatefulSet控制器的主要作用:使用Pod模版创建Pod的时候，对他们进行编号，并且按照编号顺序逐一完成创建工作;为每个Pod创建了一个固定并且稳定的DNS记录，来作为它的访问入口。

StatefulSet就保证了Pod网络标识的稳定性。但是IP会发生变化，如果访问的时候，需要使用DNS记录或者hostname的方式来访问。

Kubernetes成功地将Pod的拓扑状态(那个节点先启动，那个节点后启动)，按照Pod的名字+编码的方式固定下来了。


## 存储状态
