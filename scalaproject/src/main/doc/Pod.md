# Pod相关概念
Pod是Kubernetes项目中最小的API对象。Pod是Kubernetes项目的原子调度单位。
kubernetes项目的调度器，是统一按照Pod而非容器的资源需求进行计算的。
Docker的本质是进程。Kubernetes类似于操作系统。
pstree -g 可以查看当前系统中正在运行的进程的树状结构。
rsyslogd是负责linux操作系统里的日志处理。
容器的单进程模型不是指容器里只能运行一个进程，而是指容器没有管理多个进程的能力。
## Pod的实现原理
Pod是一个逻辑概念。
Kubernetes真正处理的，还是宿主操作系统上Linux容器的Namespace和Cgroup，并不存在一个所谓的Pod的边界或者隔离环境。
Pod是一组共享了某些资源的容器。Pod里的所有容器，共享的是同一个Network Namespace，并且可以声明共享同一个Volume。
Pod的实现需要一个中间容器，就是infra容器，它永远都是第一个被创建的容器，其他用户定义的容器，通过
Join Network Namespace的方式，于Infra容器关联在一起。
一个Pod只有一个IP地址。Pod中的容器共享同一份网络资源。Pod和Infra容器的生命周期一致。
Pod的超紧密关系是在一个容器中跑多个功能不想管的应用。
在Pod中，所有Init Container定义的容器，都会比spec.containers定义的用户容器先启动。
并且，Init Container容器会按照顺序逐一启动，而直到他们都启动并且退出，用户容器才会启动。
sidecar指的是可以在一个Pod中，启动一个辅助容器，来完成一些独立于主进程(主容器)之外的工作。 
凡是调度、网络、存储，以及安全相关的属性，基本上都是Pod级别的。
## Pod的基础概念
### NodeSelector
NodeSelector：是一个供用户将Pod于Node进行绑定的字段。用户如下:
```
apiVersion: v1
kind: Pod
...
spec:
  nodeSelector: 
     disktype: ssd
```
这意味着这个Pod只能运行在懈怠了disktype:ssd标签(Label)的节点上,否则调度失败。
### NodeName
一旦Pod的这个字段被赋值，Kubernetes项目就会认为这个Pod已经经过调度，调度的结果就是赋值的节点名字。
这个名字一般由调度器负责设置。

### HostAlias
HostAlias:定义了Pod的hosts文件里面的内容。(-i 即 stdin，-t 即 tty)。
凡是Pod中的容器要共享宿主机的Namespace，也一定是Pod级别的定义。
Pod中，只是Init Containers的生命周期，会先于所有的Containers，并且严格按照定义的顺序执行。

### ImagePullPolicy
ImagePullPolicy：定义了一个镜像拉取的策略，是conatiner级别的属性。
默认值为Always，即每次创建Pod都重新拉取一次镜像。
如果将值设置为Never/IfNotPresent，意味着Pod永远不会主动拉取这个镜像，或者只在宿主机上不存在这个镜像时才拉取。
### LifeCycle
LifeCycle定义了Container Lifecycle Hooks，是容器级别的属性，在容器状态发生变化时触发一系列钩子。
在postStart启动时，ENTRYPOINT有可能还没有结果。如果在postStart执行时出现超时或者错误，就会导致Pod处于失败状态。
preStop发生的时机则是容器被杀死之前，preStop的操作是同步的。它会阻塞当前的容器杀死流程，直到这个Hook定义操作完成之后，
才允许容器被杀死。

Pod生命中周期的变化，主要体现在Pod API对象的Status部分(pod.status.phase)。
Status的值:
>* Pending:Pod的YAML文件已经提交给Kubernetes，API对象已经被创建并保存在etcd当中。
>* Running:Pod已经调度成功，跟一个具体的节点绑定。它包含的容器都已经创建成功，至少有一个正在运行中。
>* Succeeded:Pod里面的所有容器都已经运行完毕，并且已经退出了。
>* Failed:Pod里至少有一个容器以不正常的状态退出。
>* Unknown:这是一个异常状态，Pod的状态不能持续地被kubelet汇报给kube-apiserver，这有可能是主从节点间的通信出现了问题。

Pod中Condition和Status的对应关系:
Pending(Status) == UnSchedule(Condition),调度出现了问题；
Running(Status) == Ready(Condition),Ready意味着Pod不仅已经正常启动，而且还可以对外提供服务。

## Projected Volume
Projected Volume是Kubernetes在v1.11.1之后的特性。
Projected Volume分为Secret、ConfigMap、Downward API，然后Service Account是一种特殊的Secret。
### Secret
Secret的作用是：帮助把Pod想要访问的加密数据，存放在Etcd中，然后通过在Pod的容器里挂载Volume的方式，
访问到这些Secret里保存的信息。
Secret对象要求这些数据必须要经过Base64转码的，以免出现铭文密码的安全隐患。只是进行了转码，没有加密，
如果要更安全，可以开启Secret加密的插件，增加数据的安全性。
kubelet会定时维护这些volume。

### ConfigMap
ConfigMap中保存的是不需要加密的、应用所需的配置信息。

### Downward API
让Pod里的容器直接可以获取到这个Pod API对象本身的信息。
Downward API能够获取到的信息，一定是Pod里的容器进程启动之前就能够确定下来的信息。


### Service Account
Service Account对象的作用就是Kubernetes系统内置的一种服务账号，它是Kubernetes进行权限配置的对象。
Service Account的授权信息和文件，保存在它所绑定的一个特殊的Secret对象里的，成为ServiceAccountToken。
Kubernetes提供了一个默认服务账号。任何一个运行在Kubernetes里的Pod，都可以直接使用这个默认的Service Account，
则无需显示地声明挂载它。

把Kubernetes客户端以容器的方式运行在集群里，然后使用default service account自动授权的方式，被成为InClusterConfig，
这也是推荐的运行Kubernetes API编程的授权方式。

### 健康检查
Pod里的容器定义了一个健康检查探针，kubelet会根据这个探针的返回值来决定这个容器的状态，而不是直接以容器是否运行作为依据。

### 恢复机制
Pod恢复机制:即restartPolicy，pod.spec.restartPolicy，默认值为always，即任何时期这个容器发生了异常，它一定会被重新创建。
Pod的恢复过程，永远都是发生在当前节点上，而不是跑到别的节点上。一个Pod于以节点绑定，除非这个绑定发生变化，否则Pod永远都不会离开这个节点。
如果这个宿主机宕机了，这个Pod也不会主动迁移到其他节点上。

Pod的恢复策略:
>* Always: 在任何情况下，只要容器不在运行状态，就会自动重启容器。
>* OnFailure: 只在容器异常时才会自动重启容器。
>* Never: 从来不重启容器。

Pod的设计规原理:
>* 只要Pod的restartPolicy指定的策略允许重启异常的容器，那么这个Pod就会保持Running状态，并进行容器重启。
>* 对于包含多个容器的Pod，只有它里面所有的容器都进入异常状态后，Pod才会进入Failed状态。

readlinessProbe检查结果的成功与否，决定这个Pod是不是能够被通过Service的方式访问到，并不影响Pod的声明周期。

### 控制器模型的实现
控制器模型的实现:
>* Deployment控制器从Etcd中后去到所有携带指定标签的Pod，然后统计他们的数量，这是实际状态；
>* Deployment对象的Replicas字段的值就是期望值；
>* Deployment控制器将两个状态做比较，然后比较结果，确定是创建Pod，还是删除已有的Pod。

一个ReplicaSet对象，其实就是由副本树木的定义和一个Pod模版组成的。
Deployment控制器实际操作的是ReplicaSet对象而不是Pod对象。
Deployment管理的Pod的ownerReference对象是ReplicaSet。

### Deployment、ReplicaSet、Pod的关系
Deployment、ReplicaSet、Pod的关系是层层控制的关系。
ReplicaSet负责通过控制器模式，保证系统中Pod的个数永远等于指定的个数。这正是Deployment只允许容器的restartPolicy=Always的
主要原因：只有在容器能保证自己始终是Running状态的前提下，ReplicaSet调整Pod的个数才有意义。
Deployment同样通过控制器模式，来操作ReplicaSet的个数和属性，进而实现水平扩展/收缩和滚动更新这两个编排动作。

通过kubectl get deployments查看返回值的含义:
DESIRED:用户期望Pod副本个数(scopes.replicas的值);
CURRENT:当前处于Running状态的Pod的个数;
UPDATE-TO-DATE:当前处于最新版本Pod的个数;
AVAILABLE:当前可用Pod的个数，即是Running状态，有时最新版本，并且处于Ready状态的Pod的个数。
可以通过```kubectl rollout status 容器```查看Deployment对象的状态变化。

将一个进群中正在运行的多个Pod版本，交替地逐一升级的过程，就是滚动更新。

保证服务的连续性，Deployment Controller还会确保，在任何时间串口内，只有指定比例的Pod处于离线状态。
同时，在创建新的Pod可以指定比例，默认都是DESIRED值的25%。
maxUnavailable指的是，在一次滚动中，Deployment控制器可以删除多少个旧Pod。

### Deployment 版本控制的原理
kubectl set image的指令，直接修改容器使用的镜像，这个命令的好处，不用像kubectl edit那样需求打开编辑器。
kubectl rollout undo命令，把整个Deployment回滚到上一个版本。

首先，需要使用kubectl rollout history命令，查看每次Deployment变更对应的版本。
然后，可以在kubectl rollout undo命令行最后，加上要会滚到的指定版本的版本号，就可以回滚到指定版本了。
在更新Deployment前，要执行一条kubectl rollout pause指令，然后接下来，可以随意使用kubectl edit或者
kubectl set images，修改这个Deployment的内容了，这些操作不会触发滚动更新。
在对Deployment修改操作都完成之后，只需要在执行一次kubectl rollout resume指令，然后恢复执行操作。
可以使用spec.revisionHistoryLimit 保留Deployment的版本数个数，如果设置为0，就是不能做回滚操作。

Deployment控制ReplicaSet(版本)，ReplicaSet控制Pod(副本数)。