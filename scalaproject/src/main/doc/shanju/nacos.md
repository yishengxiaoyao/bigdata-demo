# Nacos 安装

docker run --env MODE=standalone --name nacos -d -p 8848:8848 nacos/nacos-server


http://localhost:8848/nacos/index.html

默认的用户名和密码为nacos。


## 参考文献
[docker 安装 nacos/nacos-server 镜像并配置本地数据库](https://www.cnblogs.com/future-wy/p/10609374.html)