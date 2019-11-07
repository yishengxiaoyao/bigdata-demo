# Zuul架构
## Zuul网关过滤器管理模块(存储)
上传过滤器，存储到数据库中，在管理界面中进行操作。

ZuulFilterDAO用来连接数据库，持久化filter，默认使用的Cassandra，实现文件为ZuulFilterDAOCassandra。可以将其修改为Mysql。
FilterScriptManagerServlet(zuul-netflix项目)引用ZuulFilterDAO，来进行数据操作。
并且提供了具体的上传、激活、标记为其他状态的操作
(zuul-netflix-webapp/webapp/admin/filterLoader.jsp文件)。


## Zuul网关过滤器加载模块
定期扫描数据库中的过滤器，看是否有状态变化，将变更的过滤器拉到本地，检查是否有变化，如果有变化，重新加载这个新的过滤器。

初始化统计数据。
初始化监控数据。

1.获取配置管理对象。
2.获取过滤器文件路径。
3.FileLoader设置编译器GroovyCompiler。
4.FileFileManager过滤出来指定目录的groovy文件。
5.FileLoader存储groovy文件，文件编译，如果发送放到FileLoader中,存储到FilterRegistry(加载修改的groovy文件)。
6.启动ZuulFilterPoller (对应FilterPoller，线程自动循环)。
7.获取不同状态的filter(从数据库中查找，要实现IZuulFilterDao接口)，替换更新的Filter。
8.将Filter数据写入到磁盘上(根据不同的Filter类型写入到不同的目录)。
9.然后将Filter信息放入到Map中。
10.ZuulFilterRunner执行这些过滤器，实际上就是FileProcessor执行相应的filter，按照顺序执行。
11.统计执行的状态，是否成功。



## Zuul网关过滤器运行时模块
请求依次经过PreFilter、RouteFilter、PostFilter，在发生操作的时候，运行ErrorFilter。
ZuulFilterPoller过滤请求。
使用RequestContext在不同的Filter中共享信息(线程安全)。

