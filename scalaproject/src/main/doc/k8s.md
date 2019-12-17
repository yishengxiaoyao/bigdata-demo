#深入剖析kubernetes--笔记
## 容器技术概念
容器技术的核心功能，就是通过约束和修改进程的动态表现，从而为其创造出一个边界。

Cgroups技术是用来制造约束的主要手段，Namespaces技术则是用来修改进程视图的主要方法。

Mount Namespace用于让被隔离进程只看到当前Namespace里的挂在点信息;Network Namespace
用于让被隔离进程看到当前Namespace里的网络设备和配置。

容器就是一种特殊的进程而已。

虚拟机通过硬件虚拟化功能，模拟出运行一个操作系统需要的各种硬件，然后在虚拟硬件上安装了一个
新的操作系统。

容器的弊端:隔离的不彻底。

容器是运行在宿主机上的一种特殊的进程，那么多个容器之间使用的就是还是同一个宿主机的操作系统内核。
在linux内核中，有很多资源和对象是不能被Namespace化的。

Linux下的/proc目录存储是记录当前内核运行状态的一系列特殊文件，/proc文件系统不了解Cgroups限制的存在。

Mount Namespace修改的是，是容器进程对文件系统挂在点的认知。在挂载操作发生之后，进程的视图才会被改变。

Mount Namespace于其他Namespace的区别:它对容器进行视图的改变，一定是伴随着挂载操作才能生效。

chroot命令就是改变进程的根目录到指定的位置。

挂载在容器根目录上、用来为容器进行提供隔离后执行环境的文件系统，就是所谓的容器镜像，也可以成为rootfs(根文件系统)。

rootfs这是一个操作系统所包含的文件、配置和目录，不包括操作系统内核。在Linux操作系统中，这两部分是分开存放的，
操作系统只有在开机启动时才会加载指定版本的内核镜像。

同一台机器上的所有容器，都共享宿主机操作系统的内核。

容器相对于虚拟机的缺陷:虚拟机不仅可以模拟出来的硬件机器充当沙盒，而且每个沙盒里还运行着一个完成的guest os给应用随便折腾。

由于rootfs里打包的不只是应用，而是整个操作系统的文件和目录。

Union File System是将多个不同位置的目录联合挂载到同一个目录下。

镜像的层都是放置在/var/lib/docker/aufs/diff目录下，然后被联合挂载在/var/lib/docker/aufs/mnt里面。

rootfs有三个部分组成:只读层(镜像基础的文件层)、可读可写层(自定义的安装软件、增加文件等操作，增加的的rootfs)、
init层(内部层，专门存放/etc/hosts, /etc/resolv.conf等信息)。
docker commit只提交可读写层。

Docker会提供一个隐含的entrypoint，即 /bin/bash -c。

Linux Namespace创建的隔离空间虽然看不见摸不着，
但一个进程的Namespace信息在宿主机上是确确实实存在的，并且是以一个文件的方式存在。

查看容器的进程号:
```
docker inspect --format '{{ .State.Pid }}' 容器id
ls -l /proc/{pid}/ns # 连接到一个真实的Namespace文件上。
```
一个进程，可以选择加入到某个进程已有的Namespace当中，从而达到进入这个进程所在容器的目录，这正式docker exec的实现原理。

在设置网路的过程中，如果不使用host模式，就是使用容器自己的进程号，于/bin/bash的进程的网络指向同一个net。

rootfs的最上层是一个可读写层，它以copy-on-write的方式存放任何对只读层的修改，容器生命的volume挂载点，也出现在这一层。

docker commit 会将创建的文件提交上去，而不会外挂的文件提交上去。

### Kuberntes
Kubernetes有Master和Node两种节点组成，而这两种角色分别对应这控制节点和计算节点。
控制节点即Master节点，于其他三个组建组合而成:负责API服务的kube-apiserver;负责调度的kube-scheduler;
负责容器编排的kube-controller-manager;整个集群的持久化数据，则有kube-apiserver处理后保存在Etcd中。
kubelet主要负责同容器运行时打交道(CRI请求翻译成对Linux操作系统的调用)。
Kubelet还通过gRPC协议同一个叫作Device Plugin的插件进行交互。
kubelet的另一个重要功能，则是调用网络插件和存储插件为容器配置网络(CNI 容器网络接口)和持久化存储(CSI 容器存储接口)。

Service服务的主要作用，就是作为Pod的代理入口，从而代替Pod对外暴露一个固定的网络地址。

为什么不能使用docker部署kubeadm?
```
kubelet是kubernetes项目用来操作Docker等容器运行时的核心组件。
除了跟容器运行时打交道外，kubelet在配置容器网络、管理容器数据卷时，都需要直接操作宿主机。
kubelet隔着容器的Mount Namespace和文件系统，操作宿主机的文件系统，这是有困难的(kubelet的挂载操作不会传播到宿主机上)。
```

kubeadm init 的工作流程:
```
1.检进行检查工作，以确定这个机器是否可以用来部署kubernetes:在通过Preflight Checks之后,kubeadm生成Kubernetes对外提供服务
所需的各种证书和对应的目录。kubernetes默认使用https访问kube-apiserver。
kubeadm为kubernetes项目生成的证书文件都放在master节点的/etc/kubernetes/pki项目下。
证书生成后,kubeadm接下来会为其他组件生成防伪kube-apiserver所需要的配置文件,配置文件在/etc/kubernetes/*.conf。
2.kubeadm会为Master组建生成Pod配置文件。Kubenetes中有一中特殊的容器为static pod。它允许你把数据的pod的yaml文件放在一个指定的目录里。
kubelet在Kubernetes项目中的地位非常高。kubeadm会通过检查localhost:6443/healthz检查Master组件的健康状况。
3.kubeadm就会为集群生成一个bootstrap token。在token生成之后，kubeadm会将ca.crt等master节点的
重要信息,通过ConfigMap的方式保存在Etcd,供后续部署Node节点使用。这个ConfigMap的名字是cluster-info。
4.安装默认插件。
```
kubeadm join的工作流程:
```
节点需要注册到集群上，必须在集群的kube-apiserver上注册。kubeadm发送请求，从kube-apiserver中获取到授权信息。
```
通过Taint/Toleration调整Master执行Pod的策略
```
默认情况下Master节点是不允许运行用户Pod的。依赖Kubernetes的Taint/Toleration机制可以操作。
一旦某个节点被加上了一个Taint，那么所有的Pod就不能在这个节点上运行。如果个别的Pod声明Toleation，才能在这个节点上运行。
```
在使用的Kubernetes的时候，推荐使用replicas=1而不使用单独pod的主要原因是pod所在节点出现故障的时候，pod可以调度到健康的节点上，单独的pod
只能在健康节点的情况下有kubelet保证pod的健康状况。


## 参考文献
[mac上利用minikube搭建kubernetes(k8s)环境](https://www.cnblogs.com/yjmyzz/p/install-k8s-on-mac-using-minikube.html)